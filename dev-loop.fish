#!/usr/bin/env fish
# Compile and run the daemon in a loop.
# Exit code 0 = restart (recompile + rerun).
# Any other exit code = stop the loop.

while true
    cargo run -- daemon
    set code $status
    if test $code -ne 0
        echo "Daemon exited with code $code, stopping."
        break
    end
    echo "Daemon exited cleanly, restarting..."
end
