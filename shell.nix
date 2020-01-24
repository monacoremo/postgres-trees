let
  pkgs =
    let
      pinnedPkgs =
        builtins.fetchTarball {
          url = "https://github.com/nixos/nixpkgs/archive/f9c81b5c148572c2a78a8c1d2c8d5d40e642b31a.tar.gz";
          sha256 = "0ff7zhqk7mjgsvqyp4pa9xjvv9cvp3mh0ss9j9mclgzfh9wbwzmf";
        };
    in
      import pinnedPkgs {};

  postgresql =
    pkgs.postgresql_12.withPackages
      (
        ps: [
          ps.pgtap
        ]
      );
in
pkgs.stdenv.mkDerivation {
  name = "postgrest-session-example";

  buildInputs = [
    postgresql
    pkgs.entr
    pkgs.curl
    pkgs.bash
    pkgs.ephemeralpg
    pkgs.glibcLocales
  ];
}
