# artemisctl

Lightweight client tool to interact with ActiveMQ-Artemis broker via STOMP.

Usage:

consume messages from TestUser queue:
```shell
artemisctl -u artemis -p artemis -q TestUser
```

consume messages using different broker instead of localhost one:
```shell
artemisctl -b internal.demo.artemis:30202 -u artemis -p artemis -q TestUser
```

---

produce plain text messages to TestUser queue:
```shell
artemisctl -m P -u artemis -p artemis -q TestUser \                                                                                                                                                             11:14:48
-message '{"id": 1, "firstName": "Test1", "lastName": "Test"}' \
-message '{"id": 2, "firstName": "Test2", "lastName": "Test"}' \
-message '{"id": 3, "firstName": "Test3", "lastName": "Test"}'
```

produce json text messages to TestUser queue:
```shell
artemisctl -m P -u artemis -p artemis -q TestUser -t json \                                                                                                                                                             11:14:48
-message '{"id": 1, "firstName": "Test1", "lastName": "Test"}' \
-message '{"id": 2, "firstName": "Test2", "lastName": "Test"}' \
-message '{"id": 3, "firstName": "Test3", "lastName": "Test"}'
```