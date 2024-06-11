class BankingTransaction:
    """
    A class to represent a banking transaction.
    """

    def __init__(self, transaction_id, account_from, account_to, amount, timestamp):
        """
        Constructs all the necessary attributes for the BankingTransaction object.

        Args:
            transaction_id (int): Unique identifier for the transaction.
            account_from (int): Account number from which the amount is debited.
            account_to (int): Account number to which the amount is credited.
            amount (float): Amount to be transferred.
            timestamp (datetime): Time when the transaction occurred.
        """
        self.transaction_id = transaction_id
        self.account_from = account_from
        self.account_to = account_to
        self.amount = amount
        self.timestamp = timestamp

    def to_dict(self):
        """
        Converts the transaction object to a dictionary.

        Returns:
            dict: A dictionary representation of the transaction.
        """
        return {
            "transaction_id": self.transaction_id,
            "account_from": self.account_from,
            "account_to": self.account_to,
            "amount": self.amount,
            "timestamp": self.timestamp
        }
