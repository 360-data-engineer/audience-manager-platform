import json
import logging
from app import create_app
from app.models.rule_engine import Rule
from app.utils.rule_parser import RuleParser

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_subset(subset_cond, superset_cond):
    """Check if one condition dictionary is a subset of another."""
    return all(item in superset_cond.items() for item in subset_cond.items())

def main():
    """
    Analyzes all rules to identify base and composite rules.
    - For base rules, it generates and stores a direct SQL query.
    - For composite rules, it identifies dependencies, stores them, and clears the SQL query
      to force the processor to use the segment combination logic.
    """
    app = create_app()
    with app.app_context():
        from app import db
        
        logger.info("Starting advanced rule analysis for segment reuse optimization...")
        
        try:
            rules = Rule.query.order_by(Rule.id).all()
            if not rules:
                logger.warning("No rules found. Exiting.")
                return

            # Decode all rule conditions first
            decoded_rules = {}
            for r in rules:
                try:
                    decoded_rules[r.id] = {'rule': r, 'cond': json.loads(r.conditions)}
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Could not decode conditions for Rule ID {r.id}. Skipping.")

            # Process rules, starting from simplest to most complex
            for rule_id, data in sorted(decoded_rules.items(), key=lambda item: len(item[1]['cond'])):
                rule = data['rule']
                conditions = data['cond']
                logger.info(f"Analyzing Rule ID: {rule.id} ({rule.rule_name})")

                # Find component rules that this rule could be composed of
                component_candidates = []
                for other_id, other_data in decoded_rules.items():
                    if other_id == rule_id: continue # Don't compare a rule to itself
                    if is_subset(other_data['cond'], conditions):
                        component_candidates.append(other_data)
                
                # Check if the sum of component conditions equals the current rule's conditions
                # This handles AND logic (intersection)
                combined_conds = {}
                component_ids = []
                if component_candidates:
                    for cand in component_candidates:
                        combined_conds.update(cand['cond'])
                        component_ids.append(cand['rule'].id)
                
                # If a perfect composition is found
                if len(component_ids) > 1 and combined_conds == conditions:
                    logger.info(f"  -> Found composite rule. Depends on: {component_ids}")
                    depends_on_json = json.dumps(sorted(component_ids))
                    db.session.execute(
                        """
                        UPDATE segment_catalog 
                        SET sql_query = NULL, depends_on = :depends, operation = 'INTERSECTION'
                        WHERE rule_id = :rule_id
                        """,
                        {'depends': depends_on_json, 'rule_id': rule.id}
                    )
                    logger.info(f"  -> Configured Rule ID {rule.id} for segment reuse (INTERSECTION).")
                else:
                    # This is a base rule, generate SQL for it
                    logger.info(f"  -> Found base rule. Generating direct SQL query.")
                    sql_query = RuleParser.generate_segment_sql(rule.id, conditions)
                    db.session.execute(
                        """
                        UPDATE segment_catalog 
                        SET sql_query = :sql_query, depends_on = NULL, operation = NULL
                        WHERE rule_id = :rule_id
                        """,
                        {'sql_query': sql_query, 'rule_id': rule.id}
                    )
                    logger.info(f"  -> Staged SQL update for Rule ID {rule.id}.")

            db.session.commit()
            logger.info("✅ Successfully analyzed and optimized all rules.")

        except Exception as e:
            logger.error(f"A critical error occurred: {e}", exc_info=True)
            db.session.rollback()

if __name__ == "__main__":
    main()
