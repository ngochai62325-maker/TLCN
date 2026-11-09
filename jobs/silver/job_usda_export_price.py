"""Compatibility entry point for the shared contract pipeline."""
if __package__:
    from .run_all_silver import main
else:
    from run_all_silver import main

if __name__ == "__main__":
    main("usda_rice_yearbook")
