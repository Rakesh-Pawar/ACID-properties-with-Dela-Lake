import random
from datetime import datetime

from src.utils.define_spark import spark
from src.Transactions.transactions_class import BankingTransaction


def generate_dummy_transactions(num_transactions):
    """
    Generate a list of dummy banking transactions.

    Args:
        num_transactions (int): Number of transactions to generate.

    Returns:
        list: A list of dictionaries representing dummy transactions.
    """
    transactions = []
    for i in range(num_transactions):
        transaction = BankingTransaction(
            transaction_id=i,
            account_from=random.randint(1000, 5000),
            account_to=random.randint(1000, 5000),
            amount=round(random.uniform(10.0, 1000.0), 2),
            timestamp=datetime.now()
        )
        transactions.append(transaction.to_dict())
    return transactions


# Generate dummy transactions
num_transactions = 100
transactions = generate_dummy_transactions(num_transactions)

# Convert transactions to Spark DataFrame
df_transactions = spark.createDataFrame(transactions)


