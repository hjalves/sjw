# `sjw` - systemctl and journalctl wrapper

**sjw** is a WAMP daemon that wraps systemctl and journalctl
for service management.


### WAMP topics

Methods:

- `sjw.list_units`
- `sjw.query`
- `sjw.start`
- `sjw.stop`
- `sjw.restart`
- `sjw.disable`
- `sjw.enable`

Events:

- `sjw.unit.<unit>`
