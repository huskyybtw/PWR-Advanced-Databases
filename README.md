# Inside AirBnB (PWR-Advanced-Databases)

This project is set up for advanced database querying, optimization testing, and benchmarking using **Oracle 21c Express Edition**, **Flyway** (for migrations), and **Python** (for test orchestration and data loading).

## 1. Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.9+

## 2. Project Structure

- `init_scripts/`: Runs recursively on **every container startup** via the /opt/oracle/scripts/startup hook. It uses an idempotent PL/SQL block to safely ensure the airbnb user exists.
- `/migrations`: Contains your Flyway schema migrations.
- `/queries`: Folder for raw `.sql` files that you want to benchmark.

* `main.py`: The single entry point script to run your test suite, benchmarking, and database seeding.
* `scripts/`: Modular Python logic.

## 3. Getting Started

### Start the Database

To initialize the environment, run:

```bash
docker compose up -d
```

### Database Connection Details (For DBeaver / DataGrip)

- **Host:** `localhost`
- **Port:** `1521`
- **Database / Service Name:** `XEPDB1` _(Do not use SID "XE")_
- **Username:** `airbnb`
- **Password:** `Oracle123!`

### Set up Python Environment

Open your terminal in the project directory:

```bash
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
```

### Adding New Migrations (Tables, Indexes)

1. Create a new `.sql` file in the `migrations/` folder.
2. It **must** follow Flyway's naming convention: `V<Sequence_Number>__<Description>.sql` (e.g., `V2__add_index.sql`). **Note the double underscore `__`.**
3. Write your `CREATE TABLE` or `CREATE INDEX` queries inside.
4. Restart the Flyway container to apply it: `docker compose restart flyway`.

### Accessing Oracle Enterprise Manager

You can view the graphical performance dashboard at:
**URL:** `https://localhost:5500/em`
_(Make sure to use `https`. You may need to bypass the self-signed certificate warning in your browser)._

### Total Reset

If you ever need to completely wipe the database and start from scratch, run:

```bash
docker compose down -v
docker compose up -d
```
