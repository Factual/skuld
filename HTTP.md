# HTTP Interface

Skuld exposes an HTTP interface on a per-node basis. The available endpoints
are outlined below.

## Queue

### Count queues

Where `r` is the read concern.

**Example request**
```http
GET /queue/count?r=3 HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
    "count": 13,
}
```

## Tasks

### Claim a task

Where `dt` is the claim expiry delta from now in milliseconds.

**Example request**
```http
POST /tasks/claim HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json

{
    "dt": 300
}
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "task": {
    "claims": [
      {
        "completed": null,
        "end": 1387126824854,
        "start": 1387126814854
      }
    ],
    "id": "AAABQvcd+CGAAAABrYuJIPwyloY=",
    "data": "foobar"
  }
}
```

### Complete a task

Where `w` is the write concern and `cid` is the claim id, i.e. 1-indexed
position of the claim in the `claims` array.

**Example request**
```http
POST /tasks/complete/AAABQvcd+CGAAAABrYuJIPwyloY=?w=3 HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json

{
    "cid": 1
}
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
    "w": 3
}
```

### Count tasks

**Example request**
```http
GET /tasks/count HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "count": 13,
  "partitions": {
    "skuld_7": 2,
    "skuld_6": 0,
    "skuld_5": 1,
    "skuld_4": 0,
    "skuld_3": 0,
    "skuld_2": 0,
    "skuld_1": 6,
    "skuld_0": 4
  }
}
```

### Enqueue a task

Where `w` is the write concern and `task` is the task-related data object.

**Example request**
```http
POST /tasks/enqueue HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json

{
  "task": {
    "data": "foobar"
  },
  "w": 3
}
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": "AAABQvcd+CGAAAABrYuJIPwyloY=",
  "n": 3
}
```

### List tasks

**Example request**
```http
GET /tasks/list HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "tasks": [
    {
      "claims": [],
      "id": "AAABQvcd+CGAAAABrYuJIPwyloY=",
      "data": "foobar"
    },
    ...
  ]
}
```

### Get a task

Where `r` is read concern.

**Example request**
```http
GET /tasks/AAABQvcd+CGAAAABrYuJIPwyloY=?r=3 HTTP/1.1
Host: 127.0.0.1:13100
Content-Type: application/json
```

**Example response**
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "task": {
    "claims": [],
    "id": "AAABQvcd+CGAAAABrYuJIPwyloY=",
    "data": "foobar"
  },
  "n": 3
}
```
