def range_years(start_year: int, end_year: int) -> list[int]:
    """Create a sorted list of years between start and end year."""
    date_range = range(start_year, end_year + 1)
    return sorted(list(date_range))
