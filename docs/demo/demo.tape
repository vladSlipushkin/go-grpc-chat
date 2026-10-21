# Two users chat side by side: make demo writes docs/demo.gif.

Output docs/demo.gif

Set Shell "bash"
Set FontSize 18
Set Width 1500
Set Height 380
Set Padding 16
Set TypingSpeed 40ms

# Start the server, create both accounts and log in, off camera: the
# passwords are typed here, where nothing is recorded.
Hide
Type "export CHAT_JWT_SECRET=insecure-local-development-secret-0123456789" Enter
Type "server > /tmp/server.log 2>&1 &" Enter
Sleep 1s
Type "chat register -user alice" Enter
Sleep 500ms
Type "correct horse" Enter
Sleep 300ms
Type "correct horse" Enter
Sleep 1s
Type "chat register -user bob" Enter
Sleep 500ms
Type "correct horse" Enter
Sleep 300ms
Type "correct horse" Enter
Sleep 1s
Type "tmux -f /dev/null new-session 'chat -user alice' \; set status off \; split-window -h 'chat -user bob'" Enter
Sleep 1500ms
Type "correct horse" Enter
Ctrl+B
Type "o"
Sleep 300ms
Type "correct horse" Enter
Sleep 1500ms
Show

# Alice, on the left.
Type "/join general" Enter
Sleep 1s

# Bob, on the right.
Ctrl+B
Type "o"
Type "/join general" Enter
Sleep 1s
Type "hi alice! this travels over one bidi stream" Enter
Sleep 1500ms

Ctrl+B
Type "o"
Type "hey bob, the room orders every message" Enter
Sleep 1500ms
Type "/rooms" Enter
Sleep 1500ms

Ctrl+B
Type "o"
Type "/leave" Enter
Sleep 1500ms

Ctrl+B
Type "o"
Type "bye!" Enter
Sleep 3s
