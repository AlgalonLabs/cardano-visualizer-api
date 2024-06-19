class CurrencyConverter:
    LOVELACE_PER_ADA = 1_000_000

    @staticmethod
    def lovelace_to_ada(lovelace: float) -> float:
        return float(lovelace / CurrencyConverter.LOVELACE_PER_ADA)

    @staticmethod
    def ada_to_lovelace(ada: float) -> float:
        return float(ada * CurrencyConverter.LOVELACE_PER_ADA)