# Setup

Install python dependencies in your virtualenv

```bash
pip install -r requirements.txt
```

Run example with memory as backend.

```bash
uvicorn example.app:app
```

You can also install broadcaster locally using `pip install -e .`.

In order to run the app with different backends, you have to set the env
`BROADCAST_URL` and start the docker services.

| Backend  | Env                                                          | Service command              |
| -------- | ------------------------------------------------------------ | ---------------------------- |
| kafka    | `export BROADCAST_URL=kafka://localhost:9092`                | `docker-compose up kafka`    |
| redis    | `export BROADCAST_URL=redis://localhost:6379`                | `docker-compose up redis`    |
| postgres | `export BROADCAST_URL=postgres://localhost:5432/broadcaster` | `docker-compose up postgres` |
