# app/utils/dependency_finder.py
import logging
from typing import List, Dict, Any, Tuple, Optional, Set
from ..models.rule_engine import Rule
from .. import db

logger = logging.getLogger(__name__)

def _conditions_to_set(conditions: List[Dict[str, Any]]) -> Optional[Set[tuple]]:
    """Converts a list of condition dicts to a canonical set of sorted tuples for comparison."""
    if not isinstance(conditions, list):
        return None
    try:
        return {tuple(sorted(cond.items())) for cond in conditions}
    except (TypeError, AttributeError):
        logger.warning(f"Could not parse conditions, they may contain unhashable types: {conditions}")
        return None

def find_best_dependency(new_conditions: List[Dict[str, Any]], rule_id_to_exclude: Optional[int] = None) -> Optional[Tuple[List[int], str, List[Dict[str, Any]]]]:
    """
    Finds the optimal set of existing rules whose conditions are subsets of the new rule's conditions.
    This supports multi-level and many-to-one dependencies.

    Args:
        new_conditions: The list of conditions for the new/updated rule.
        rule_id_to_exclude: The ID of the rule being updated, to avoid self-dependency.

    Returns:
        A tuple containing (list_of_dependency_rule_ids, operation, remaining_conditions),
        or None if no suitable dependencies are found.
    """
    all_conditions_set = _conditions_to_set(new_conditions)
    if not all_conditions_set or len(all_conditions_set) < 1:
        return None

    # Fetch all active rules, sorted by the number of conditions descending.
    # This makes our greedy algorithm more effective.
    query = Rule.query.filter(Rule.is_active == True).order_by(db.func.json_array_length(Rule.conditions).desc())
    if rule_id_to_exclude:
        query = query.filter(Rule.id != rule_id_to_exclude)
    
    potential_dependencies = query.all()

    found_dependencies = []
    remaining_conditions_set = all_conditions_set.copy()

    for existing_rule in potential_dependencies:
        # No point in checking if we have no conditions left to match
        if not remaining_conditions_set:
            break

        existing_conditions_set = _conditions_to_set(existing_rule.conditions)
        if not existing_conditions_set:
            continue

        # Check if the existing rule is a proper subset of the *remaining* conditions
        if existing_conditions_set.issubset(remaining_conditions_set):
            logger.info(f"Found dependency match: Rule {existing_rule.id} covers {len(existing_conditions_set)} conditions.")
            found_dependencies.append(existing_rule.id)
            remaining_conditions_set -= existing_conditions_set

    # We only create a compound rule if we found at least one dependency
    # AND doing so is more efficient than creating a new base rule from scratch.
    # A dependency is only useful if it replaces at least one condition AND leaves other conditions.
    if found_dependencies and len(all_conditions_set) > len(remaining_conditions_set):
        logger.info(f"Final dependencies for rule: {found_dependencies}. Remaining conditions: {len(remaining_conditions_set)}")

        # Convert the remaining set of tuples back to a list of dictionaries
        final_remaining_conditions = [cond for cond in new_conditions if tuple(sorted(cond.items())) in remaining_conditions_set]

        return found_dependencies, 'intersection', final_remaining_conditions

    logger.info("No effective dependencies found. Creating as a base rule.")
    return None
