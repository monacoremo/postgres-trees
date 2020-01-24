
\set ON_ERROR_STOP on

\echo 'Setting up tree table and functions...'

create table lineitems
    ( lineitem_id   serial primary key
    , parent_id     int references lineitems(lineitem_id) on delete cascade
    , position      int not null
    , label         text not null

    , check (position >= 0)
    , constraint unique_lineitem_position
        unique (parent_id, position)
        deferrable initially immediate
    );

comment on table lineitems is
    'Financial statements line items.';

comment on column lineitems.position is
    'Position of the item among its siblings.';

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

create function lineitems(root_id int)
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
                    where lineitem_id = root_id
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
                where items.lineitem_id != root_id
                order by path_
    $$;

comment on function lineitems is
    'Returns the tree of line items starting from a root node.';

\echo 'Loading fixtures...'

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


\echo 'Running tests...'

select setval('lineitems_lineitem_id_seq', max(lineitem_id)) from lineitems;

select (repeat('  ', level_) || number || ' ' || label) as before from lineitems(0);

select insert_lineitem_after(4, 'test');
select delete_lineitem(7);
select move_lineitem_after(1, 13);
select insert_lineitem_first(0, 'first!');
select move_lineitem_first(4, 9);

select (repeat('  ', level_) || number || ' ' || label) as after from lineitems(0);

select (repeat('  ', level_) || number || ' ' || label) as subtree from lineitems(9);

create function random_tree(root_id int, depth_ int, fanout int)
    returns void
    language plpgsql
    as $$
        declare
            child_id int;
        begin
            for i in 1..fanout loop
                insert into lineitems(parent_id, position, label)
                    values (root_id, i -1, 'Line item ' || coalesce(root_id, 0) || '-' || i)
                    returning lineitem_id into strict child_id;

                if depth_ > 1 then
                    perform random_tree(child_id, depth_ - 1, fanout);
                end if;
            end loop;
        end
    $$;


insert into lineitems(position, label)
    values (0, 'root for generated tree')
    returning lineitem_id
\gset lineitemid

select * from lineitems where parent_id is null;

select * from random_tree(16, 4, 10);

select count(*) from lineitems(16);

select (repeat('  ', level_) || number || ' ' || label) as "generated tree"
    from lineitems(16);

create function benchmark()
    returns void
    language plpgsql
    as $$
        declare
            id int;
        begin
            for id
                in
                    select lineitem_id
                        from lineitems tablesample bernoulli (10) repeatable (1)
                loop
                    perform delete_lineitem(id);
            end loop;
        end
    $$;

select count(*) from lineitems;

select delete_lineitem(lineitem_id) from lineitems tablesample bernoulli (1) repeatable (1);
-- select benchmark();

select count(*) from lineitems;

