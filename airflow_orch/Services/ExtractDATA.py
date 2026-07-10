from loguru import logger
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ExtractDataFromDB:
    @staticmethod
    def check_table(
        connection_id: str,
        schema_name: str,
        table_name: str,
    ) -> bool:
        hook = PostgresHook(postgres_conn_id=connection_id)

        query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_name = %s
        );
        """

        result = hook.get_first(
            query,
            parameters=(schema_name, table_name),
        )[0]

        logger.info(f"Checking if table '{schema_name}.{table_name}' exists.")

        return result