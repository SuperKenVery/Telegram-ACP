{ self }:
{
  config,
  lib,
  ...
}:

let
  cfg = config.services.telegram-acp;
in
{
  meta.maintainers = [ ];

  options.services.telegram-acp = {
    enable = lib.mkEnableOption "telegram-acp daemon";

    package = lib.mkOption {
      type = lib.types.nullOr lib.types.package;
      default = null;
      defaultText = lib.literalExpression ''
        inputs.telegram-acp.packages.''${pkgs.stdenv.hostPlatform.system}.default
      '';
      example = lib.literalExpression "pkgs.telegram-acp";
      description = ''
        Package providing the `telegram-acp` executable. Defaults to this
        flake's package for the current host system.
      '';
    };

    botToken = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11";
      description = ''
        Telegram bot token to write into the generated config file.

        This value is stored in the Nix store. Prefer
        `services.telegram-acp.botTokenFile` for real deployments.
      '';
    };

    botTokenFile = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "\${config.sops.secrets.telegram-acp-bot-token.path}";
      description = ''
        File containing the Telegram bot token. When set, the service wrapper
        exports it as `TELEGRAM_ACP_BOT_TOKEN` before starting the daemon.
      '';
    };

    chatId = lib.mkOption {
      type = lib.types.nullOr lib.types.int;
      default = null;
      example = 123456789;
      description = "Telegram chat ID used by the daemon.";
    };

    socketPath = lib.mkOption {
      type = lib.types.str;
      default = "/tmp/telegram-acp.sock";
      example = "\${config.xdg.runtimeDir}/telegram-acp.sock";
      description = "Unix socket path used for CLI-to-daemon IPC.";
    };

    defaultAgent = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      example = "codex";
      description = ''
        Default agent name. This may be omitted when exactly one agent is
        configured.
      '';
    };

    agents = lib.mkOption {
      type = lib.types.attrsOf lib.types.str;
      default = { };
      example = {
        codex = "codex --acp";
        claude = "claude-agent-acp";
      };
      description = ''
        Agent command table. Each attribute becomes a config table with a
        `cmd` value, for example `agents.codex = "codex --acp"` becomes
        `[codex].cmd`.
      '';
    };

    extraConfig = lib.mkOption {
      type = lib.types.attrs;
      default = { };
      example = {
        telegraph_author = "Your Name";
      };
      description = "Additional TOML configuration merged into the generated config file.";
    };

    logLevel = lib.mkOption {
      type = lib.types.str;
      default = "info";
      example = "telegram_acp=debug,info";
      description = "Value for the daemon's `RUST_LOG` environment variable.";
    };
  };

  config = lib.mkIf cfg.enable (
    let
      pkgs = config._module.args.pkgs;
      package =
        if cfg.package != null then
          cfg.package
        else
          self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      tomlFormat = pkgs.formats.toml { };

      agentConfig = lib.mapAttrs (_: cmd: { inherit cmd; }) cfg.agents;
      baseConfig = lib.filterAttrs (_: value: value != null) {
        bot_token = cfg.botToken;
        chat_id = cfg.chatId;
        socket_path = cfg.socketPath;
        default_agent = cfg.defaultAgent;
      };

      configFile = tomlFormat.generate "telegram-acp-config.toml" (
        baseConfig // agentConfig // cfg.extraConfig
      );

      escapedTokenFile = lib.optionalString (cfg.botTokenFile != null) (
        lib.escapeShellArg cfg.botTokenFile
      );

      daemonScript = pkgs.writeShellScript "telegram-acp-daemon" ''
        set -eu

        ${lib.optionalString (cfg.botTokenFile != null) ''
          if [ ! -r ${escapedTokenFile} ]; then
            echo "telegram-acp: botTokenFile is not readable: ${cfg.botTokenFile}" >&2
            exit 1
          fi

          export TELEGRAM_ACP_BOT_TOKEN="$(cat ${escapedTokenFile})"
        ''}

        exec ${lib.getExe package} daemon
      '';
    in
    {
      assertions = [
        {
          assertion = cfg.botToken != null || cfg.botTokenFile != null;
          message = "services.telegram-acp requires either botToken or botTokenFile.";
        }
        {
          assertion = cfg.chatId != null;
          message = "services.telegram-acp.chatId is required.";
        }
        {
          assertion = cfg.agents != { };
          message = "services.telegram-acp.agents must contain at least one agent command.";
        }
        {
          assertion = cfg.defaultAgent == null || builtins.hasAttr cfg.defaultAgent cfg.agents;
          message = "services.telegram-acp.defaultAgent must match a configured agent.";
        }
        {
          assertion = cfg.defaultAgent != null || builtins.length (builtins.attrNames cfg.agents) == 1;
          message = "services.telegram-acp.defaultAgent is required when multiple agents are configured.";
        }
      ];

      home.packages = [ package ];

      xdg.configFile."telegram-acp/config.toml".source = configFile;
    }
    // {
      systemd.user.services.telegram-acp = lib.mkIf config.systemd.user.enable {
        Unit = {
          Description = "Telegram ACP daemon";
          After = [ "network-online.target" ];
          Wants = [ "network-online.target" ];
        };

        Service = {
          ExecStart = daemonScript;
          Environment = [ "RUST_LOG=${cfg.logLevel}" ];
          Restart = "on-failure";
          RestartSec = 5;
        };

        Install.WantedBy = [ "default.target" ];
      };
    }
    // {
      launchd.agents.telegram-acp = lib.mkIf config.launchd.enable {
        enable = true;
        config = {
          Label = "org.telegram-acp.daemon";
          ProgramArguments = [ daemonScript ];
          EnvironmentVariables = {
            RUST_LOG = cfg.logLevel;
          };
          KeepAlive = true;
          RunAtLoad = true;
          ProcessType = "Background";
        };
      };
    }
  );
}
