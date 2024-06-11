from src.Transactions.simulate_transactions import generate_dummy_transactions
from src.utils.define_spark import spark
from src.utils.implement_delta_lake import delta_table


def perform_isolated_transactions(delta_table, transactions):
    """
    Perform isolated transactions to ensure snapshot isolation.

    Args:
        delta_table (DeltaTable): The DeltaTable to perform operations on.
        transactions (list): A list of transactions to be performed.
    """
    try:
        # Start a new snapshot for transaction 1
        snapshot1 = delta_table.toDF().filter("amount < 500").collect()
        print("Snapshot 1:")
        for row in snapshot1:
            print(row)

        # Perform operations within a single transaction
        for transaction in transactions:
            df = spark.createDataFrame([transaction])
            delta_table.alias("t").merge(
                df.alias("s"),
                "t.transaction_id = s.transaction_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # Start a new snapshot for transaction 2
        snapshot2 = delta_table.toDF().filter("amount < 500").collect()
        print("Snapshot 2:")
        for row in snapshot2:
            print(row)

        print("Transactions completed with isolation.")
    except Exception as e:
        print(f"Transaction failed: {e}")


# Generate new set of transactions to simulate isolated transactions
new_transactions = generate_dummy_transactions(5)
perform_isolated_transactions(delta_table, new_transactions)


"""
Isolation ensures that the operations of one transaction are isolated from the operations of other transactions.
 In Delta Lake, this is achieved through snapshot isolation.
"""