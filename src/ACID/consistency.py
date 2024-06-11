
from delta import DeltaTable
from src.utils.implement_delta_lake import delta_table


def check_consistency(delta_table: DeltaTable):
    """
    Check consistency by ensuring no transaction has an amount greater than a threshold.

    Args:
        delta_table (DeltaTable): The DeltaTable to check.
    """
    try:
        threshold = 10000
        inconsistent_transactions = delta_table.toDF().filter(f"amount > {threshold}")
        if inconsistent_transactions.count() == 0:
            print("All transactions are consistent.")
        else:
            inconsistent_transactions_count = inconsistent_transactions.count()
            print(f"Inconsistent transactions count: {inconsistent_transactions_count}")
            print("Found inconsistent transactions:")
            inconsistent_transactions.show()
    except Exception as e:
        print(f"Consistency check failed: {e}")


# Perform consistency check
check_consistency(delta_table)


"""
Consistency ensures that a transaction can only bring the database from one valid state to another. 
In the context of Delta Lake, this ensures that any constraints defined are always met.
"""