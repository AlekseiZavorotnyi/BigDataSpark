from pathlib import Path
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen, urlretrieve
import base64
import os

from pyspark.sql import SparkSession


BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

POSTGRES_JDBC = {
    "path": "postgresql-42.7.3.jar",
    "url": "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar",
}


def load_config(path=BASE_DIR / "config.yaml"):
    config = {}
    current_section = None

    for raw_line in Path(path).read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue

        if not line.startswith(" "):
            current_section = line.rstrip(":")
            config[current_section] = {}
            continue

        key, value = line.strip().split(":", 1)
        config[current_section][key] = value.strip().strip('"').strip("'")

    return config


def _writable_jars_dir():
    candidates = []
    env_dir = os.environ.get("SPARK_JARS_DIR")
    if env_dir:
        candidates.append(Path(env_dir))
    candidates.extend([Path("/opt/spark/jars"), PROJECT_DIR / "jars"])

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            test_file = candidate / ".write-test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink()
            return candidate
        except OSError:
            continue

    raise RuntimeError("No writable directory for JDBC jars")


def ensure_postgres_driver():
    jars_dir = _writable_jars_dir()
    jar_path = jars_dir / POSTGRES_JDBC["path"]

    if not jar_path.exists():
        print(f"Downloading PostgreSQL JDBC driver to {jar_path}")
        try:
            urlretrieve(POSTGRES_JDBC["url"], jar_path)
        except (HTTPError, URLError) as exc:
            raise RuntimeError(
                "Could not download PostgreSQL JDBC driver. "
                "Check internet access inside the Spark container."
            ) from exc

    return jar_path


def create_spark(app_name):
    postgres_jar = ensure_postgres_driver()
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.jars", str(postgres_jar))
        .config("spark.driver.extraClassPath", str(postgres_jar))
        .config("spark.executor.extraClassPath", str(postgres_jar))
        .getOrCreate()
    )


def postgres_properties(config):
    pg = config["postgres"]
    return {
        "user": pg["user"],
        "password": pg["password"],
        "driver": pg["driver"],
    }


def execute_jdbc_statements(spark, url, user, password, driver, statements):
    jvm = spark.sparkContext._gateway.jvm
    jvm.java.lang.Class.forName(driver)

    conn = jvm.java.sql.DriverManager.getConnection(url, user, password)
    try:
        statement = conn.createStatement()
        try:
            for sql in statements:
                statement.execute(sql)
        finally:
            statement.close()
    finally:
        conn.close()


def clickhouse_query(ch_config, query, retries=30, retry_delay=2):
    url = ch_config["http_url"]
    data = query.encode("utf-8")
    last_error = None

    for _ in range(retries):
        request = Request(url, data=data, method="POST")
        user = ch_config.get("user", "")
        password = ch_config.get("password", "")
        if user:
            token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
            request.add_header("Authorization", f"Basic {token}")

        try:
            with urlopen(request, timeout=30) as response:
                return response.read().decode("utf-8")
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            sleep(retry_delay)

    raise RuntimeError(f"ClickHouse query failed after retries: {query[:160]}") from last_error


def recreate_clickhouse_table(ch_config, table_name, schema_sql):
    clickhouse_query(ch_config, f"DROP TABLE IF EXISTS {table_name}")
    clickhouse_query(ch_config, schema_sql)


def write_clickhouse_json(ch_config, table_name, dataframe):
    rows = dataframe.toJSON().collect()
    if not rows:
        return

    payload = "\n".join(rows)
    clickhouse_query(ch_config, f"INSERT INTO {table_name} FORMAT JSONEachRow\n{payload}", retries=3)
