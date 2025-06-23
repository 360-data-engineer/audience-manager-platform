# app/utils/rule_parser.py
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

class RuleParser:
    """
    Parses rule conditions and generates the corresponding SQL query for segment creation.
    """

    @staticmethod
    def generate_segment_sql(rule_id: int, conditions: Dict[str, Any]) -> str:
        """
        Generates a SQL query to create a user segment based on a set of conditions.
        This query aggregates data from both UPI and Credit Card transaction tables.

        Args:
            rule_id: The ID of the rule.
            conditions: A dictionary defining the filtering conditions.

        Returns:
            A string containing the generated SQL query.
        """
        logger.info(f"Generating SQL for Rule ID: {rule_id} with conditions: {conditions}")

        base_query = """
        WITH all_transactions AS (
            SELECT 
                user_id,
                amount,
                transaction_date,
                category,
                city_tier,
                'UPI' as transaction_type
            FROM upi_transactions_raw
            
            UNION ALL
            
            SELECT 
                user_id,
                amount,
                transaction_date,
                category,
                city_tier,
                'CREDIT_CARD' as transaction_type
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

        where_conditions = []
        having_conditions = []

        # Timeframe conditions (e.g., last 30 days)
        if 'timeframe_days' in conditions:
            where_conditions.append(f"transaction_date >= date('now', '-{int(conditions['timeframe_days'])} days')")

        # Transaction amount threshold (applied to individual transactions)
        if conditions.get('type') == 'transaction_amount' and 'value' in conditions and 'operator' in conditions:
             where_conditions.append(f"amount {conditions['operator']} {float(conditions['value'])}")
        
        # City Tier filter
        if 'city_tier_in' in conditions and conditions['city_tier_in']:
            tiers = ', '.join([f"'{tier}'" for tier in conditions['city_tier_in']])
            where_conditions.append(f"city_tier IN ({tiers})")
            
        # Total Spend threshold (applied after aggregation)
        if 'total_spend_gt' in conditions:
            having_conditions.append(f"SUM(amount) > {float(conditions['total_spend_gt'])}")
        
        if 'total_spend_lt' in conditions:
            having_conditions.append(f"SUM(amount) < {float(conditions['total_spend_lt'])}")

        # Transaction count threshold (applied after aggregation)
        if 'transaction_count_gt' in conditions:
            having_conditions.append(f"COUNT(user_id) > {int(conditions['transaction_count_gt'])}")
            
        if 'transaction_count_lt' in conditions:
            having_conditions.append(f"COUNT(user_id) < {int(conditions['transaction_count_lt'])}")


        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        having_clause = f"HAVING {' AND '.join(having_conditions)}" if having_conditions else ""
        
        final_sql = base_query.format(where_clause=where_clause, having_clause=having_clause)
        
        # Clean up whitespace and newlines
        final_sql = " ".join(final_sql.strip().split())
        
        logger.debug(f"Generated SQL for Rule ID {rule_id}: {final_sql}")
        
        return final_sql

from datetime import datetime, timedelta
from typing import Dict, Any, List

class RuleParser:
    @staticmethod
    def parse_conditions(conditions: Any) -> str:
        """Convert UI filter conditions to SQL WHERE clause"""
        where_clauses = []
        
        if isinstance(conditions, list):
            # New format from dynamic UI: list of condition dicts
            raw_conditions = [
                cond for cond in conditions
                if cond.get('field') in ['start_date', 'end_date', 'city_tier', 'transaction_type', 'amount']
            ]
            for cond in raw_conditions:
                field = cond['field']
                operator = cond.get('operator', '=') # Default operator
                value = cond['value']

                if field == 'start_date':
                    where_clauses.append(f"transaction_date >= '{value}'")
                elif field == 'end_date':
                    where_clauses.append(f"transaction_date <= '{value}'")
                else:
                    sql_value = f"'{value}'" if isinstance(value, str) else value
                    where_clauses.append(f"{field} {operator} {sql_value}")

        elif isinstance(conditions, dict):
            # Legacy format: flat dictionary
            if conditions.get('start_date') or conditions.get('end_date'):
                date_conditions = []
                if conditions.get('start_date'):
                    date_conditions.append(f"transaction_date >= '{conditions['start_date']}'")
                if conditions.get('end_date'):
                    date_conditions.append(f"transaction_date <= '{conditions['end_date']}'")
                where_clauses.append(f"({' AND '.join(date_conditions)})")

            if conditions.get('city_tier'):
                where_clauses.append(f"city_tier = {conditions['city_tier']}")

            transaction_type = conditions.get('transaction_type', 'all')
            if transaction_type != 'all':
                where_clauses.append(f"transaction_type = '{transaction_type.upper()}'")

            if conditions.get('amount') is not None:
                operator = conditions.get('amount_operator', '=')
                where_clauses.append(f"amount {operator} {conditions['amount']}")
        
        return " AND ".join(where_clauses) if where_clauses else "1=1"
    
    @staticmethod
    def generate_segment_sql(rule_id: int, conditions: Any) -> str:
        """Generate SQL to create a segment based on rule conditions"""
        where_clause = RuleParser.parse_conditions(conditions)
        
        sql = f"""
        WITH user_transactions AS (
            -- UPI Transactions
            SELECT 
                user_id,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                'UPI' as transaction_type
            FROM upi_transactions_raw
            WHERE {where_clause}
            GROUP BY user_id
            
            UNION ALL
            
            -- Credit Card Transactions
            SELECT 
                user_id,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                'CREDIT_CARD' as transaction_type
            FROM credit_card_transactions_raw
            WHERE {where_clause}
            GROUP BY user_id
        )
        SELECT 
            user_id,
            COUNT(*) as total_transactions,
            SUM(total_amount) as total_spent,
            (
                SELECT GROUP_CONCAT(t2.transaction_type, ',')
                FROM (SELECT DISTINCT user_id, transaction_type FROM user_transactions) t2
                WHERE t2.user_id = user_transactions.user_id
            ) as transaction_types
        FROM user_transactions
        GROUP BY user_id
        """
        
        # Add transaction count filter if specified
        min_transactions = None
        if isinstance(conditions, list):
            for cond in conditions:
                if cond.get('field') == 'min_transactions':
                    min_transactions = cond.get('value')
                    break
        elif isinstance(conditions, dict):
            min_transactions = conditions.get('min_transactions')

        if min_transactions:
            sql += f" HAVING COUNT(*) >= {min_transactions}"
        
        return sql