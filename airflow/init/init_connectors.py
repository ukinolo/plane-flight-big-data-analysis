from airflow.models import Connection
from airflow.utils.db import provide_session

@provide_session
def create_spark_connection(conn_id, host, session=None):
    conn_type = "Spark"

    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn:
        print(f"Connection '{conn_id}' already exists.")
        return
    
    new_conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
    )

    session.merge(new_conn)
    session.commit()
    print(f"Connection '{conn_id}' created successfully.")


if __name__ == "__main__":
    create_spark_connection("spark_custom", "spark://spark-master:7077")