I want to write a rust project, telegram-acp. On one side, it would use ACP (agent client protocol) to communicate with coding agents like claude code. On the other side, it would act as a telegram bot.

For the acp part, you can use the agent-client-protocol crate. We would have some built in agents that map to command line args (for example, claude code maps to `claude-agent-acp`.)

For the telegram part, we would use two amazing apis.

1. The sendMessage api with disable_notification. Only the first agent message and the end msg or error msg should have notification, the rest should be silent.
2. The topic api. createForumTopic allows you to create a topic in a private chat (despite its name). We would have a main topic that interacts with the bot, and a separate topic for each single agent invokation.

We will have a cli and a daemon. The daemon manages all agent sessions connected to telegram, and a cli can tell the daemon spawn new sessions. (telegram can also create new sessions by /new /path/to/project). The daemon acts as a telegram bot and communicates with all the agents via ACP. Whenever there's a new agent message, it sends a telegram message. If there's long code changes, it creates a telegraph post with all the changed file names and content, where the contents are collapsible. If it fails or finishes, sends a message with notification.

Am I clear? If so, write the plan. Anything unclear, ask me.
