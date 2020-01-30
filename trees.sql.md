
# Ordered trees in PostgreSQL

This is an example on how to model and work with ordered trees in Postgres.
Let's say, for example, that we want to model the structure of financial
statements in a way that we can easily adapt and extend it. Financial
statement line items usually form a hierarchy:

![Revenues in the 2018 financial statements of Bershire Hathaway Inc.](Berkshire_Hathaway_Revenues_2018.png)

Source: [Berkshire Hathaway Inc., Form 10-K for 2018, Page K-66](https://www.berkshirehathaway.com/2018ar/201810-k.pdf)

Note that the line items have been listed in a deliberate order, that is not
necessarily alphabetical or by size. We can model this strucutre with
[ordered trees](https://en.wikipedia.org/wiki/Tree_(data_structure)#Ordered_tree).

## Basic setup

This Markdown document is also a literate SQL document, that is you get an
executable SQL script if you filter out the SQL code blocks. We tell `psql`
to stop if any error occurs and begin a new transaction.

```sql
\set ON_ERROR_STOP on

begin;
```

See [`run.sh`](./run.sh) for how to run this file.

## Data definition

To model the hierarchical structure of the revenues in our example, we need
to keep track of the line item that each line item belongs to (the parent) and
the order of the line items that all have the same parent (siblings).

```sql
create table lineitems
    ( lineitem_id   serial primary key
    , parent_id     int references lineitems(lineitem_id)
    , position      int not null
    , label         text not null

    , constraint valid_lineitem_parent check (lineitem_id != parent_id)
    , constraint valid_lineitem_position check (position >= 0)
    , constraint unique_lineitem_position
        unique (parent_id, position)
        deferrable initially immediate
    );

comment on table lineitems is
    'Financial statements line items.';

comment on column lineitems.position is
    'Position of the item among its siblings.';
```

Based on the contraints that we set up on our new table, only valid ordered
tree structures can be represented. It's impossible for a line item to have
a non-existing parent (`references`) and no line items can be in the same
position as one of its siblings.

Root nodes are reperesented by line items where the `parent_id` is `null`. An
alternative would be to disallow `null` values and to define root nodes as
ones that have themselves as a parent. However, it would be quite cumbersome
to create such nodes and we would need to support the process with a trigger.
So this seems like a better solution.

The `positions` column might have holes, e.g. a item might be at position
3 and the next one at 5, leaving a hole at position 4. This is not an issue
for us, as our data still describes a valid order of the siblings. It's
impossible to represent an invalide state.

## Data queries

To work with our hierarchical line items, we will often need a listing of them
that includes the level and the number each item. In the example above, the
level is represented by the identation of each item. For example, if the item
'Insurance and Other' is at level 1, 'Leasing revenues' is at level 2; they are
in the first and third position among their siblings, respectively.

### Line item type

Let's create a new type to represent line items and their position in the
hierarchy:

```sql
create type lineitem as
    ( lineitem_id int
    , parent_id int
    , number bigint
    , label text
    , level_ int
    , path_ integer[]
    );

comment on type lineitem is
    'Line item, including its level and path in the tree of line items.';
```

The `path` will be used as an array of positions, which can be useful to
order the line items.

### Line item subtrees

The subtrees of a parent line item are all the trees that descend from it. We will
use a recursive `with`-query (or Common Table Expression) to select all those
items for a given parent.

```sql
create function lineitem_subtrees(lineitem_id int)
    returns setof lineitem
    language sql
    as $$
        with recursive
            items as (
                select
                        lineitem_id,
                        parent_id,
                        position,
                        label,
                        1 level_,
                        array[]::integer[] path_
                    from lineitems
                    where lineitems.lineitem_id = lineitem_subtrees.lineitem_id
                union all
                    select
                            children.lineitem_id,
                            children.parent_id,
                            children.position,
                            children.label,
                            items.level_ + 1 level_,
                            items.path_ || children.position path_
                        from items, lineitems children
                        where items.lineitem_id = children.parent_id
            )
            select
                    lineitem_id,
                    parent_id,
                    row_number() over (
                        partition by parent_id
                        order by position
                    ),
                    label,
                    level_,
                    path_
                from items
                where items.lineitem_id != lineitem_subtrees.lineitem_id
                order by path_
    $$;

comment on function lineitem_subtrees is
    'Returns the tree of line items starting from a root node.';
```

### Creating new line items

#### Inserting new line items as the first sibling after an existing line item

```sql
create function insert_lineitem_after(lineitem_id int, label text)
    returns int
    language plpgsql
    as $$
        declare
            target lineitems;
            new_lineitem_id int;
        begin
            -- find the item that the new item should be inserted after
            select * from lineitems
                where lineitems.lineitem_id = insert_lineitem_after.lineitem_id
                into strict target;

            -- make room for our new item
            set constraints unique_lineitem_position deferred;

            update lineitems set position = position + 1
                where
                    parent_id = target.parent_id
                    and position > target.position;

            set constraints unique_lineitem_position immediate;

            -- insert our new item
            insert into lineitems(parent_id, position, label)
                values (target.parent_id, target.position + 1, insert_lineitem_after.label)
                returning lineitems.lineitem_id
                into new_lineitem_id;

            return new_lineitem_id;
        end;
    $$;

comment on function insert_lineitem_after is
    'Insert a new line item as the first sibling after the given line item.';
```

#### Inserting new line items as the first child of a line item

```sql
create function insert_lineitem_first(parent_id int, label text)
    returns int
    language plpgsql
    as $$
        declare
            new_lineitem_id int;
        begin
            -- make room for the new item
            set constraints unique_lineitem_position deferred;

            update lineitems set position = position + 1
                where
                    lineitems.parent_id = insert_lineitem_first.parent_id;

            set constraints unique_lineitem_position immediate;

            -- insert the new item
            insert into lineitems(parent_id, position, label)
                values (insert_lineitem_first.parent_id, 0, insert_lineitem_first.label)
                returning lineitems.lineitem_id into new_lineitem_id;

            return new_lineitem_id;
        end;
    $$;

comment on function insert_lineitem_first is
    'Insert a new line item as the first child of another item.';
```

### Deleting line items

#### Deleting a leaf line item and closing the hole it might have left behind

```sql
create function delete_lineitem(lineitem_id int)
    returns void
    language plpgsql
    as $$
        declare
            target lineitems;
        begin
            -- find the item to be deleted
            select * from lineitems
                where lineitems.lineitem_id = delete_lineitem.lineitem_id
                into strict target;

            -- delete the item
            delete from lineitems
                where lineitems.lineitem_id = delete_lineitem.lineitem_id;

            -- close the hole left by the deleted item
            set constraints unique_lineitem_position deferred;

            update lineitems set position = position - 1
                where
                    parent_id = target.parent_id
                    and position > target.position;

            set constraints unique_lineitem_position immediate;
        end;
    $$;

comment on function delete_lineitem is
    'Delete the given line item.';
```

This function will not work on any items that have any children, as our
`references` constraint will prevent the deletion. We could specify
`on delete cascade` on it, but we would rather be explicit about such
destructive deletes and will define a separate function to implement it.

#### Deleting the subtrees of a line item

```sql
create function delete_lineitem_subtrees(lineitem_id int)
    returns void
    language sql
    as $$
        delete from lineitems
            where lineitem_id in (
                select lineitem_id
                from lineitem_subtrees(delete_lineitem_subtrees.lineitem_id)
            )
    $$;

comment on function delete_lineitem_subtrees is
    'Delete the subtrees of the given line item.';
```

#### Deleting a line item and its subtrees

```sql
create function delete_lineitem_including_subtrees(lineitem_id int)
    returns void
    language sql
    as $$
        select delete_lineitem_subtrees(lineitem_id);
        select delete_lineitem(lineitem_id);
    $$;

comment on function delete_lineitem_including_subtrees is
    'Delete the subtrees of the given line item and the given lineitem itself.';
```

### Moving line items

#### Moving line items to be the first sibling after an existing line item

To move a line item to any other location in our hierarchy, we need to perform
the following steps:

1. Make room for the line item at its intended new position, moving all the
   items with the same or higher position by one.
2. Move the line item there.
3. Close the hole in the `position`s it might have left behind.

```sql
create function move_lineitem_after(lineitem_id int, target_lineitem_id int)
    returns void
    language plpgsql
    as $$
        declare
            item_to_move lineitems;
            target lineitems;
        begin
            -- find the item that we want to move
            select * from lineitems
                where lineitems.lineitem_id = move_lineitem_after.lineitem_id
                into strict item_to_move;

            -- find the item that we want to move our item after
            select * from lineitems
                where lineitems.lineitem_id = target_lineitem_id
                into strict target;

            -- make room for the item to be moved
            set constraints unique_lineitem_position deferred;

            update lineitems set position = position + 1
                where
                    parent_id = target.parent_id
                    and position > target.position;

            set constraints unique_lineitem_position immediate;

            -- move the item
            set constraints unique_lineitem_position deferred;

            update lineitems
                set
                    position = target.position + 1,
                    parent_id = target.parent_id
                where
                    lineitems.lineitem_id = move_lineitem_after.lineitem_id;

            set constraints unique_lineitem_position immediate;

            -- close the hole left by the moved item
            update lineitems set position = position - 1
                where
                    parent_id = item_to_move.parent_id
                    and position > item_to_move.position;
        end;
    $$;

comment on function move_lineitem_after is
    'Move a line item to be the first sibling after another.';
```

#### Moving a line item to be the first child of a parent line item

```sql
create function move_lineitem_first(lineitem_id int, parent_id int)
    returns void
    language plpgsql
    as $$
        declare
            item_to_move lineitems;
        begin
            -- find the item that we want to move
            select * from lineitems
                where lineitems.lineitem_id = move_lineitem_first.lineitem_id
                into strict item_to_move;

            -- make room for the item to be moved
            set constraints unique_lineitem_position deferred;

            update lineitems set position = position + 1
                where
                    lineitems.parent_id = move_lineitem_first.parent_id;

            set constraints unique_lineitem_position immediate;

            -- move the item
            update lineitems
                set
                    position = 0,
                    parent_id = move_lineitem_first.parent_id
                where
                    lineitems.lineitem_id = move_lineitem_first.lineitem_id;

            -- close the hole left by the moved item
            set constraints unique_lineitem_position deferred;

            update lineitems set position = position - 1
                where
                    lineitems.parent_id = item_to_move.parent_id
                    and position > item_to_move.position;

            set constraints unique_lineitem_position immediate;
        end;
    $$;

comment on function move_lineitem_first is
    'Move a line item to be the first child of another item.';
```

## Tests

Now that our table, type and functions are all set up, let's play with our new
API for a bit.

### Savepoint

We will not want to keep the results of our following experiements, so we
set up a save point that we can revert to later.

```sql
savepoint before_tests;
```

Let's also turn `timing` on, so that we can see how much time each of the following
queries takes:

```sql
\timing
```

### Fixture

Let's load some example data in our table:

```sql
insert into lineitems(lineitem_id, parent_id, position, label)
    values
        (0, null, 0, 'Balance sheet'),
        (1, 0, 0, 'Assets'),
        (2, 1, 0, 'Current assets'),
        (3, 2, 0, 'Accounts receivable'),
        (4, 2, 1, 'Cash and cash equivalents'),
        (5, 2, 2, 'Inventories'),
        (6, 1, 1, 'Non-current assets'),
        (7, 6, 0, 'Property, plant and equipment'),
        (8, 6, 1, 'Financial assets'),
        (9, 0, 1, 'Liabilities'),
        (10, 9, 0, 'Accounts payable'),
        (11, 9, 1, 'Provisions'),
        (12, 9, 2, 'Financial liabilities'),
        (13, 0, 2, 'Equity');
```

As we set the `lineitem_id` of each item manually, the sequence for the column
has not been used or incremented and any future insert would fail, as it would
conflict with our first line item. Accordingly, we need to reset the sequence:

```sql
select setval('lineitems_lineitem_id_seq', max(lineitem_id)) from lineitems;
```

We'll also tell Postgres to analyze our current data, so that it can create
efficient query plans:

```sql
analyze;
```

### Displaying the tree of line items

We can visualize the trees by indenting each item based on its level and by
enumerating siblings:

```sql
select (repeat('  ', level_) || number || ' ' || label) as before
    from lineitem_subtrees(0);
```

Tbis should also work for line items that are not root items:

```sql
select (repeat('  ', level_) || number || ' ' || label) as subtree
    from lineitem_subtrees(9);
```

### Modifying line items

```sql
select insert_lineitem_after(4, 'test');
select delete_lineitem(7);
select move_lineitem_after(1, 13);
select insert_lineitem_first(0, 'first!');
select move_lineitem_first(4, 9);

select (repeat('  ', level_) || number || ' ' || label) as after
    from lineitem_subtrees(0);
```

## Loading a larger tree

```sql
create function generate_tree(root_ids int[], height int, branching_factor int)
    returns void
    language sql
    as $$
        with
            items as (
                insert into lineitems(parent_id, position, label)
                    select
                            roots.lineitem_id,
                            positions.position,
                            'generated ' || roots.lineitem_id || '-' || positions.position
                        from
                            unnest(root_ids) roots(lineitem_id),
                            generate_series(0, branching_factor - 1) positions(position)
                    returning lineitem_id
            )
        select
            case when height > 1 then
                (select
                    generate_tree(array_agg(lineitem_id), height - 1, branching_factor)
                    from items)
            end
    $$;

comment on function generate_tree is
    'Generate a tree of the given height and with the given branching factor.';
```

```sql
insert into lineitems(position, label)
    values (0, 'root for generated tree')
    returning lineitem_id
\gset lineitemid

-- The new tree should have a lineitem_id of 16.
select * from lineitems where parent_id is null;

\echo 'Generating trees...'
select generate_tree(array[16], height => 4, branching_factor => 10);
```

The size of the generated tree can be tuned in the function call above.
The number of generated nodes, starting from one root node, can be calculated as
`sum(branching_factor**(i + 1) for i in range(height))` or
`(branching_factor**(height + 1) - 1) / (branching_factor - 1)`.

Given that we loaded a lot of new data, we should give Postgres an opportunity
to catch up.

```sql
analyze;
```

```sql
select count(*) from lineitem_subtrees(16);

select (repeat('  ', level_) || number || ' ' || label) as "generated tree"
    from lineitem_subtrees(16)
    limit 100;

create function benchmark()
    returns void
    language plpgsql
    as $$
        begin
            raise notice 'Inserting items as a first child of random items...';
            perform insert_lineitem_first(lineitem_id, 'insert_first ' || row_number() over ())
                from lineitems tablesample bernoulli (1) repeatable (2);

            raise notice 'Inserting random items as first sibling after another item...';
            perform insert_lineitem_after(lineitem_id, 'insert_after')
                from lineitems tablesample bernoulli (1) repeatable (3);

            raise notice 'Moving random items to be the first sibling after another item...';
            for i in 1..1000 loop
                perform move_lineitem_after(moved.lineitem_id, target.lineitem_id)
                    from
                        ( select * from lineitems
                            tablesample bernoulli (1) repeatable (i) limit 1
                        ) moved,
                        ( select * from lineitems
                            tablesample bernoulli (1) repeatable (i+100) limit 1
                        ) target;
            end loop;

            raise notice 'Deleting some random items...';
            for i in 1..100 loop
                perform delete_lineitem_including_subtrees(lineitem_id)
                    from lineitems tablesample bernoulli (1) repeatable (i)
                    limit 1;
            end loop;
        end
    $$;

select count(*) from lineitems;

select benchmark();

select count(*) from lineitems;

select (repeat('  ', level_) || number || ' ' || label) as "generated tree"
    from lineitem_subtrees(0);

rollback to savepoint before_tests;

commit;
```
