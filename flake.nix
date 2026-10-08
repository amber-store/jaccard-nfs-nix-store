{
  description = "jaccard-nfs-nix-store";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-26.05";

    systems.url = "github:nix-systems/default";

    gonixgo = {
      url = "github:draganm/gonixgo/v0.2.0";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.systems.follows = "systems";
    };

  };

  outputs = { self, nixpkgs, systems, gonixgo, ... }@inputs:
    let
      eachSystem = f:
        nixpkgs.lib.genAttrs (import systems)
        (system: f system nixpkgs.legacyPackages.${system});
    in {

      # Evaluating a gonixgo package runs `gonixgo resolve` through
      # builtins.exec, so every command that touches `packages` needs
      #   --option allow-unsafe-native-code-during-evaluation true
      packages = eachSystem (system: pkgs:
        let
          # Buildbarn's packages need Go 1.27, which is not yet the Go of
          # this nixpkgs.
          goEnv = gonixgo.lib.mkGoEnv {
            inherit pkgs;
            go = pkgs.buildPackages.go_1_27;
          };
        in {
          # The sidecar. It is one for Linux: built for anything else the
          # command only says so.
          default = goEnv.buildGoApplication {
            pname = "jaccard-nfs-nix-store";
            src = ./.;
            subPackages = [ "cmd/jaccard-nfs-nix-store" ];
            meta.license = pkgs.lib.licenses.lgpl3Only;
          };
        });

      # The shell has Go and nothing that is built from the tree, so it
      # needs no evaluation option.
      devShells = eachSystem (system: pkgs: {
        default = pkgs.mkShell {
          shellHook = ''
            # Set here the env vars you want to be available in the shell
          '';
          hardeningDisable = [ "all" ];

          packages = with pkgs; [ go_1_27 ];
        };
      });
    };
}
