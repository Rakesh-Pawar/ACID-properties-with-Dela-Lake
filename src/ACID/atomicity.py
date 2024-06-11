from src.Transactions.simulate_transactions import generate_dummy_transactions
from src.utils.define_spark import spark
from src.utils.implement_delta_lake import delta_table


def perform_atomic_transaction(oldDF, transactions):
    """
    Perform a series of operations as a single atomic transaction.

    Args:
        :param transactions: The DeltaTable to perform operations on.
        :param oldDF: A list of transactions to be performed.
    """
    try:
        # Perform operations within a single transaction
        for transaction in transactions:
            newDF = spark.createDataFrame([transaction])
            oldDF.alias("t").merge(
                newDF.alias("s"),
                "t.transaction_id = s.transaction_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print("Transaction successful.")
    except Exception as e:
        print(f"Transaction failed: {e}")


# Generate new set of transactions to simulate atomic transaction
new_transactions = generate_dummy_transactions(5)
df = delta_table(spark)
perform_atomic_transaction(df, new_transactions)


"""
Atomicity ensures that a series of operations either all succeed or all fail.
In the context of Delta Lake, this means that either all changes in a transaction are committed, or none are.

"""