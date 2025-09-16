import mysql.connector
import logging
from typing import Dict
import json

from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar, Iterable,
)

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
class VeDBClient:
    def __init__(self, host, port, user, password, database, table_name, dimension, charset='utf8mb4',
                 metadata_columns: Optional[Dict[str, str]] = None):
        assert table_name.isidentifier(), f"Invalid table name: {table_name}" # Ensure table_name is valid
        # Ensure 'uid' is not in metadata columns
        if metadata_columns and "uid" in metadata_columns:
            raise ValueError("metadata_columns must not contain the reserved column name 'uid'")

        self.conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset=charset
        )
        self.cursor = self.conn.cursor()
        self.table_name = table_name
        self.dimension = int(dimension)
        logger.info(f"✓ Successfully connected to database {database} @ {host}:{port}")

        # Build additional metadata columns
        metadata_columns_sql = ""
        if metadata_columns:
            metadata_columns_sql = ",\n".join(
                f"`{col}` {col_type if col_type else 'VARCHAR(512)'} NOT NULL"
                for col, col_type in metadata_columns.items()
            )

        # Compose CREATE TABLE statement
        create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                   uid int primary key auto_increment,
                   sid VARCHAR(255) UNIQUE NOT NULL,
                   vector vector({self.dimension}) NOT NULL,
                   text TEXT NOT NULL
                   {',' if metadata_columns_sql else ''}{metadata_columns_sql}
                )
        """
        self.cursor.execute(create_table_sql)
        self.conn.commit()

        # Check if an index named 'vectoridx' already exists to avoid duplicate creation
        self.cursor.execute(f"SHOW INDEX FROM `{self.table_name}` WHERE Key_name = 'vectoridx'")
        if not self.cursor.fetchone():
            scalar_fields = ",".join(metadata_columns.keys()) if metadata_columns else ""
            secondary_engine_attr = f'{{"scalar_fields": "{scalar_fields}"}}' if scalar_fields else '{}'
            create_ann_index_sql = f"""
                    CREATE ANN INDEX vectoridx ON `{self.table_name}`(vector) SECONDARY_ENGINE_ATTRIBUTE = '{secondary_engine_attr}';
                    """
            self.cursor.execute(create_ann_index_sql)
            self.conn.commit()

    def add_document(self, document: dict) -> None:
        # Parse metadata JSON string into a Python dict
        metadata = json.loads(document["metadata"]) if isinstance(document["metadata"], str) else document["metadata"]

        # Get existing columns from the table
        self.cursor.execute(f"SHOW COLUMNS FROM `{self.table_name}`")
        existing_columns = {row[0] for row in self.cursor.fetchall()}

        # Filter metadata to only include columns that exist in the table, prevent no exist data
        filtered_metadata = {k: v for k, v in metadata.items() if k in existing_columns}

        # 类型转换确保一致性
        if hasattr(self, "metadata_columns") and self.metadata_columns:
            for key, value in filtered_metadata.items():
                col_type = self.metadata_columns.get(key, "").upper()

                if value is None:
                    # None 直接保留，让驱动处理成 NULL
                    filtered_metadata[key] = None
                    continue

                try:
                    if "CHAR" in col_type or "TEXT" in col_type:
                        if isinstance(value, (dict, list)):
                            # dict/list 转 JSON 字符串，保持中文
                            str_value = json.dumps(value, ensure_ascii=False)
                        else:
                            str_value = str(value)

                        str_value = str_value.replace('"', '')  # 去掉双引号
                        str_value = str_value.replace("'", "")  # 去掉单引号

                        filtered_metadata[key] = str_value
                    elif "INT" in col_type:
                        # bool 转 int 避免 True/False 插入失败
                        if isinstance(value, bool):
                            filtered_metadata[key] = int(value)
                        else:
                            filtered_metadata[key] = int(value)
                    elif "FLOAT" in col_type or "DOUBLE" in col_type:
                        filtered_metadata[key] = float(value)
                    elif "BOOL" in col_type:
                        filtered_metadata[key] = int(bool(value))  # 显式转 0/1
                    else:
                        # 其他类型默认转字符串
                        if isinstance(value, (dict, list)):
                            str_value = json.dumps(value, ensure_ascii=False)
                        else:
                            str_value = str(value)

                        str_value = str_value.replace('"', '').replace("'", '')
                        filtered_metadata[key] = str_value
                except Exception as e:
                    raise ValueError(f"字段 {key} 的值 {value} 与类型 {col_type} 不匹配: {e}")
                # 其他类型不做转换

        # Prepare columns and values for insert
        columns = ['sid', 'vector', 'text'] + list(filtered_metadata.keys())
        # Use to_vector(%s) for the 'vector' column in VALUES
        placeholders_list = ['%s' if col != 'vector' else 'TO_VECTOR(%s)' for col in columns]
        placeholders = ', '.join(placeholders_list)
        column_names = ', '.join(f"`{col}`" for col in columns)

        # Build update fields
        update_fields = [
            f"{col}=VALUES({col})" for col in filtered_metadata.keys()
        ]
        update_fields += [
            "vector=VALUES(vector)",
            "text=VALUES(text)"
        ]

        # Build SQL
        sql = f"""
              INSERT INTO `{self.table_name}` ({column_names})
              VALUES ({placeholders})
              ON DUPLICATE KEY UPDATE
              {', '.join(update_fields)};
            """

        # Values for insert
        values = [document["id"], document["vector"], document["text"]] + list(filtered_metadata.values())

        # Execute insert
        self.cursor.execute(sql, values)
        self.conn.commit()

    def delete(self, sid: str) -> None:
        sql = f"""DELETE FROM `{self.table_name}` WHERE sid = %s"""
        self.cursor.execute(sql, (sid,))
        self.conn.commit()

    def update_document(self, document: dict) -> None:
        """
        Update a record based on the sid field.
        """
        # Parse metadata JSON string into a Python dict
        metadata = json.loads(document["metadata"]) if isinstance(document["metadata"], str) else document["metadata"]

        # Get existing columns from the table
        self.cursor.execute(f"SHOW COLUMNS FROM `{self.table_name}`")
        existing_columns = {row[0] for row in self.cursor.fetchall()}

        # Filter metadata to only include columns that exist in the table
        filtered_metadata = {k: v for k, v in metadata.items() if k in existing_columns}

        # Build SET clauses for metadata
        set_clauses = [
            f"{col}=%s" for col in filtered_metadata.keys()
        ]
        set_clauses += [
            "vector=TO_VECTOR(%s)",
            "text=%s"
        ]
        set_clause_sql = ", ".join(set_clauses)

        # Build SQL
        sql = f"""
            UPDATE `{self.table_name}`
            SET {set_clause_sql}
            WHERE sid = %s;
        """

        # Prepare values
        values = list(filtered_metadata.values()) + [
            document["vector"],
            document["text"],
            document["id"]  # still using document["id"] to fill sid
        ]

        # Execute update
        self.cursor.execute(sql, values)
        self.conn.commit()

    def get(self, id: str) -> Optional[dict]:
        # Fetch all columns except the auto-increment primary key 'id'
        self.cursor.execute(f"SHOW COLUMNS FROM `{self.table_name}`")
        columns = [row[0] for row in self.cursor.fetchall() if row[0] != "uid"]
        column_list = ', '.join(f"`{col}`" for col in columns)

        sql = f"SELECT {column_list} FROM `{self.table_name}` WHERE sid = %s"
        self.cursor.execute(sql, (id,))
        row = self.cursor.fetchone()

        if row is None:
            return None

        # Build metadata excluding sid, text, vector
        metadata = {
            col: row[idx]
            for idx, col in enumerate(columns)
            if col not in {"sid", "text", "vector"} and row[idx] not in (None, "")
        }

        return {
            "id": row[columns.index("sid")],
            "text": row[columns.index("text")],
            "metadata": metadata
        }

    def similarity_search_with_score_by_vector(
            self,
            embedding: List[float],
            k: int = 4,
            filter: Optional[dict] = None):
        # Get all columns except auto-increment uid
        self.cursor.execute(f"SHOW COLUMNS FROM `{self.table_name}`")
        columns = [row[0] for row in self.cursor.fetchall() if row[0] != "uid"]
        column_list = ', '.join(f"`{col}`" for col in columns)

        filter_sql = ""
        if filter:
            try:
                filter_sql = "WHERE " + self.parse_metadata_filter_to_sql(filter)
            except Exception as e:
                raise ValueError(f"Invalid filter: {e}")

        sql_stmt = f"""
            SELECT {column_list}, 
            1 - cosine_distance(vector, TO_VECTOR(%s)) AS similarity_score
            FROM `{self.table_name}` FORCE INDEX (vectoridx)
            {filter_sql}
            ORDER BY cosine_distance(vector, TO_VECTOR(%s))
            LIMIT %s;
        """

        self.cursor.execute(sql_stmt, (str(embedding), str(embedding), k))
        rows = self.cursor.fetchall()

        results = []
        for row in rows:
            # Reconstruct metadata from all columns except sid, text, vector
            metadata = {
                col: row[idx]
                for idx, col in enumerate(columns)
                if col not in {"sid", "text", "vector"} and row[idx] not in (None, "")
            }
            results.append((
                {
                    "id": row[columns.index("sid")],
                    "text": row[columns.index("text")],
                    "metadata": metadata
                },
                row[-1]  # similarity_score
            ))
        return results

    def parse_metadata_filter_to_sql(self, filter_obj: dict, json_col: str = "metadata") -> str:
        COMPARISONS_TO_NATIVE = {
            "$eq": "=",
            "$ne": "!=",
            "$lt": "<",
            "$lte": "<=",
            "$gt": ">",
            "$gte": ">=",
        }

        def parse_condition(key: str, value: Any) -> str:
            # Use column name directly
            if isinstance(value, dict):
                clauses = []
                for op, val in value.items():
                    if op in COMPARISONS_TO_NATIVE:
                        clauses.append(f"{key} {COMPARISONS_TO_NATIVE[op]} {repr(val)}")
                    elif op == "$in":
                        vals = ", ".join(repr(v) for v in val)
                        clauses.append(f"{key} IN ({vals})")
                    elif op == "$nin":
                        vals = ", ".join(repr(v) for v in val)
                        clauses.append(f"{key} NOT IN ({vals})")
                    elif op == "$between":
                        low, high = val
                        clauses.append(f"{key} BETWEEN {repr(low)} AND {repr(high)}")
                    elif op == "$exists":
                        if val:
                            clauses.append(f"{key} IS NOT NULL")
                        else:
                            clauses.append(f"{key} IS NULL")
                    # elif op == "$like":
                    #     clauses.append(f"{key} LIKE {repr(val)}")
                    # elif op == "$ilike":
                    #     clauses.append(f"LOWER({key}) LIKE LOWER({repr(val)})")
                    else:
                        raise ValueError(f"Unsupported operator: {op}")
                return " AND ".join(clauses)
            else:
                return f"{key} = {repr(value)}"

        def parse_logical(obj: Any) -> str:
            if not isinstance(obj, dict):
                raise ValueError("Filter expression must be a dict")

            clauses = []

            for key, value in obj.items():
                if key == "$and":
                    and_clauses = [parse_logical(v) for v in value]
                    clauses.append(f"({' AND '.join(and_clauses)})")
                elif key == "$or":
                    or_clauses = [parse_logical(v) for v in value]
                    clauses.append(f"({' OR '.join(or_clauses)})")
                elif key == "$not":
                    not_clause = parse_logical(value)
                    clauses.append(f"(NOT ({not_clause}))")
                else:
                    clauses.append(parse_condition(key, value))

            return " AND ".join(clauses)

        return parse_logical(filter_obj)

    def clear_all(self):
        """Delete all data from the connected client's table."""
        try:
            self.cursor.execute(f"DELETE FROM `{self.table_name}`")
            self.conn.commit()

            logger.info(f"All data has been deleted from table `{self.table_name}`.")
        except mysql.connector.Error as e:
            logger.info(f"[✗] Deletion failed: {e}")

    def close(self):
        """Close the database connection and cursor."""
        try:
            self.cursor.close()
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass