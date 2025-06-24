# app/utils/rule_parser.py
import logging
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)

class RuleParser:
    """
    Parses rule conditions from a list of condition objects and generates an efficient SQL query.
    """

    FIELD_TO_COLUMN_MAP = {
        'transaction_amount': 'amount',
        'city_tier': 'city_tier',
        'transaction_date': 'transaction_date',
        'total_spend': 'total_spent',
        'transaction_count': 'total_transactions'
    }

    AGGREGATE_FIELDS = ['total_spend', 'transaction_count']

    @staticmethod
    def _parse_to_clauses(conditions: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
        """
        Parses a list of condition objects into WHERE and HAVING clauses.

        Args:
            conditions: A list of dictionaries, each defining a filtering condition.

        Returns:
            A tuple containing two lists: where_conditions and having_conditions.
        """
        where_conditions = []
        having_conditions = []

        if not isinstance(conditions, list):
            logger.warning(f"Conditions format is not a list: {conditions}")
            return [], []

        for condition in conditions:
            field = condition.get('field')
            operator = condition.get('operator')
            value = condition.get('value')
            value2 = condition.get('value2')

            if not all([field, operator, value is not None]):
                logger.warning(f"Skipping malformed condition: {condition}")
                continue

            allowed_operators = ['>', '<', '=', '>=', '<=', '!=', 'IN', 'NOT IN', 'BETWEEN']
            if operator.upper() not in allowed_operators:
                logger.warning(f"Skipping condition with invalid operator: {operator}")
                continue

            column_name = RuleParser.FIELD_TO_COLUMN_MAP.get(field)
            if not column_name:
                logger.warning(f"Skipping condition with unknown field: {field}")
                continue
            
            clause = ""
            if operator.upper() == 'BETWEEN':
                if value2 is None:
                    logger.warning(f"Skipping BETWEEN operator with missing second value: {condition}")
                    continue
                sql_value1 = f"'{value}'"
                sql_value2 = f"'{value2}'"
                clause = f"{column_name} BETWEEN {sql_value1} AND {sql_value2}"
            else:
                sql_value = f"'{value}'" if isinstance(value, str) else str(value)
                if operator.upper() in ['IN', 'NOT IN']:
                    if isinstance(value, list) and value:
                        formatted_values = []
                        for v in value:
                            if isinstance(v, (int, float)):
                                formatted_values.append(str(v))
                            else:
                                formatted_values.append(f"'{v}'")
                        sql_value = f"({', '.join(formatted_values)})"
                    else:
                        logger.warning(f"Skipping IN/NOT IN operator with non-list or empty value: {value}")
                        continue
                
                clause = f"{column_name} {operator.upper()} {sql_value}"

            if field in RuleParser.AGGREGATE_FIELDS:
                if field == 'total_spend':
                    clause = f"SUM(amount) {operator.upper()} {sql_value}"
                elif field == 'transaction_count':
                    clause = f"COUNT(user_id) {operator.upper()} {sql_value}"
                having_conditions.append(clause)
            else:
                where_conditions.append(clause)

        return where_conditions, having_conditions

    @staticmethod
    def generate_segment_sql(rule_id: int, conditions: List[Dict[str, Any]]) -> str:
        """
        Generates an efficient SQL query to create a user segment.
        """
        logger.info(f"Generating SQL for Rule ID: {rule_id} with conditions: {conditions}")

        base_query = """
        WITH all_transactions AS (
            SELECT user_id, amount, transaction_date, category, city_tier, 'UPI' as transaction_type
            FROM upi_transactions_raw
            UNION ALL
            SELECT user_id, amount, transaction_date, category, city_tier, 'CREDIT_CARD' as transaction_type
            FROM credit_card_transactions_raw
        ),
        filtered_transactions AS (
            SELECT *
            FROM all_transactions
            {where_clause}
        )
        SELECT
            ft.user_id,
            COUNT(ft.user_id) as total_transactions,
            SUM(ft.amount) as total_spent,
            GROUP_CONCAT(DISTINCT ft.transaction_type) as transaction_types
        FROM filtered_transactions ft
        GROUP BY ft.user_id
        {having_clause}
        """

        where_conditions, having_conditions = RuleParser._parse_to_clauses(conditions)

        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        having_clause = f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""

        final_sql = base_query.format(where_clause=where_clause, having_clause=having_clause)
        final_sql = " ".join(final_sql.strip().split())

        logger.debug(f"Generated SQL for Rule ID {rule_id}: {final_sql}")

        return final_sql