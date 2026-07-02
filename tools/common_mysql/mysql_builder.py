# 构建github仓库对应数据表的sql语句

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_SQL_SET_FILE_PATTERN = "*_sql_set.py"
_REQUIRED_PLACEHOLDERS = ("{database_name}", "{table_name}")
_DEFAULT_SQL_SET_DIR = Path(__file__).resolve().parent / "mysql_table_module"


def _is_sql_template(value: object) -> bool:
    """判断模块变量是否为可用的建表 SQL 模板。"""
    if not isinstance(value, str):
        return False
    text = value.strip()
    if "CREATE TABLE" not in text.upper():
        return False
    return all(placeholder in text for placeholder in _REQUIRED_PLACEHOLDERS)


def _load_module_from_file(file_path: Path) -> ModuleType:
    module_name = f"tools.common_mysql.mysql_table_module.{file_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载 SQL 模块: {file_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _extract_sql_mapping_from_module(module: ModuleType) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for name, value in vars(module).items():
        if name.startswith("_"):
            continue
        if _is_sql_template(value):
            mapping[name] = value
    return mapping


def load_sql_set_from_directory(directory_path: str | Path | None = None) -> dict[str, str]:
    """
    自动扫描目录下所有 *_sql_set.py 文件，加载建表 SQL 模板。

    识别规则:
    - 文件名匹配 ``*_sql_set.py``
    - 模块内非私有字符串变量
    - 内容包含 ``CREATE TABLE`` 且含 ``{database_name}``、``{table_name}`` 占位符

    :param directory_path: SQL 模块目录；默认 ``tools/common_mysql/mysql_table_module``
    :return: ``{table_type: sql_template}``
    """
    directory = Path(directory_path) if directory_path else _DEFAULT_SQL_SET_DIR
    if not directory.is_dir():
        raise FileNotFoundError(f"SQL 目录不存在: {directory}")

    sql_set_files = sorted(directory.glob(_SQL_SET_FILE_PATTERN))
    if not sql_set_files:
        raise FileNotFoundError(
            f"目录 {directory} 下未找到匹配 {_SQL_SET_FILE_PATTERN} 的文件"
        )

    create_sql_mapping: dict[str, str] = {}
    for file_path in sql_set_files:
        module = _load_module_from_file(file_path)
        module_mapping = _extract_sql_mapping_from_module(module)
        for key, sql in module_mapping.items():
            if key in create_sql_mapping:
                raise ValueError(
                    f"重复的 table_type {key!r}，文件 {file_path.name} 与已加载模板冲突"
                )
            create_sql_mapping[key] = sql

    if not create_sql_mapping:
        raise ValueError(f"目录 {directory} 中未解析到任何 SQL 模板")

    return create_sql_mapping


class MysqlSqlBuilder:
    def __init__(self, create_sql_mapping: dict=None):
        # 初始化sql语句 模板映射
        self.create_sql_mapping = create_sql_mapping if create_sql_mapping else load_sql_set_from_directory()


    def create_table_sql(self, database_name: str, table_name: str, table_type: str=None):
        """
        创建github仓库对应数据表的sql语句
        :param table_name: 数据表名称
        :param table_type: 数据表类型
        :return: 创建对应github仓库的数据表的sql语句
        """
        try:
            sql = self.create_sql_mapping[table_type].format(database_name=database_name, table_name=table_name)
        except KeyError:
            raise ValueError(f"不支持的表类型: {table_type}")
        return sql


    def select_data_sql(self, database_name: str, table_name: str, 
                        column_list: list=None, condition_params: str=None):
        """
        查询数据sql语句
        :param database_name: 数据库名称
        :param table_name: 数据表名称
        :param table_type: 数据表类型
        :param column_list: 列列表
        :param condition_params: 条件参数 示例
        :return: 查询数据的sql语句
        """
        try:
            if condition_params:
                condition_sql = f"WHERE {condition_params}"
            else:
                condition_sql = ""
            if column_list:
                column_sql = f", ".join(column_list)
            else:
                column_sql = "*"
            base_sql = f"SELECT {column_sql} FROM {database_name}.{table_name} {condition_sql}"
            return base_sql
        except Exception as e:
            raise ValueError(f"查询数据sql语句构建失败: {str(e)}")


    
    def insert_data_sql(self, database_name: str, table_name: str, column_list: list=None, update_columns: list=None):
        """
        插入数据sql语句
        :param database_name: 数据库名称
        :param table_name: 数据表名称
        :param column_list: 字段列表
        :return: 插入数据的sql语句
        """
        try:
            if column_list:
                column_sql = f", ".join(column_list)
            else:
                column_sql = "*"
            vars_sql = ", ".join([f"%({s})s" for s in column_list]) if column_list else "()"
            if update_columns:
                update_sql = f", ".join([f"{dim} = VALUES({dim})" for dim in update_columns])
            else:
                update_sql = ""
            sql = f"""
            INSERT INTO {database_name}.{table_name} ({column_sql}) VALUES ({vars_sql})
            ON DUPLICATE KEY UPDATE
            {update_sql};
            """
            return sql
        except Exception as e:
            raise ValueError(f"插入数据sql语句构建失败: {str(e)}")


    def update_data_sql(self, database_name: str, table_name: str, column_list: list=None, condition_params: str=None):

        """
        更新数据sql语句
        :param database_name: 数据库名称
        :param table_name: 数据表名称
        :param column_list: 列列表
        :param condition_params: 条件参数
        :return: 更新数据的sql语句
        """
        try:
            if condition_params:
                condition_sql = f"WHERE {condition_params}"
            else:
                condition_sql = "" 
            if column_list:
                column_sql = f", ".join([f"{dim} = %({dim})s" for dim in column_list])
            else:
                raise ValueError(f"字段列表不能为空, 请检查参数: column_list={column_list}")
            sql = f"""
            UPDATE {database_name}.{table_name} SET {column_sql} {condition_sql};
            """
            return sql
        except Exception as e:
            raise ValueError(f"更新数据sql语句构建失败: {str(e)}")

    def delete_data_sql(self, database_name: str, table_name: str, condition_params: str=None):
        """
        删除数据sql语句
        :param database_name: 数据库名称
        :param table_name: 数据表名称
        :param condition_params: 条件参数
        :return: 删除数据的sql语句
        """
        try:
            if condition_params:
                condition_sql = f"WHERE {condition_params}"
            else:
                condition_sql = ""
            sql = f"""
            DELETE FROM {database_name}.{table_name} {condition_sql};
            """
            return sql
        except Exception as e:
            raise ValueError(f"删除数据sql语句构建失败: {str(e)}")



    def build_mysql_sql(self, sql_type: str, database_name: str, table_name: str, 
            table_type: str=None, column_list: list=None, values: list=None, condition_params: str=None,
            update_columns: list=None):
        """
        构建mysql sql语句
        :param sql_type: sql语句类型
        :param database_name: 数据库名称
        :param table_name: 数据表名称
        :param table_type: 数据表类型
        :param column_list: 列列表
        :param values: 值列表
        :param condition_params: 条件参数
        :return: 构建的mysql sql语句
        """
        if sql_type == 'create':
            return self.create_table_sql(database_name, table_name, table_type)

        elif sql_type == 'select':
            return self.select_data_sql(database_name, table_name, column_list, condition_params)

        elif sql_type == 'insert':
            return self.insert_data_sql(database_name, table_name, column_list, update_columns)
        
        elif sql_type == 'update':
            return self.update_data_sql(database_name, table_name, column_list, condition_params)
        
        elif sql_type == 'delete':
            return self.delete_data_sql(database_name, table_name)
        else:
            raise ValueError(f"不支持的sql语句类型: {sql_type}")
        

# if __name__ == "__main__":
#     mapping = load_sql_set_from_directory()
#     print(f"loaded {len(mapping)} sql templates:")
#     for name in sorted(mapping):
#         print(f"  - {name}")