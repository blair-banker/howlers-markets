class MarketsCoreError(Exception):
    """Base exception for markets-core."""
    pass


class DataSourceError(MarketsCoreError):
    """Error from external data source."""
    pass


class SeriesNotFoundError(DataSourceError):
    """Requested series not found."""
    pass


class StorageError(MarketsCoreError):
    """Storage read/write error."""
    pass


class ConfigurationError(MarketsCoreError):
    """Invalid configuration."""
    pass


class InsufficientDataError(MarketsCoreError):
    """Required data for classification was not available at the given as-of date."""
    pass