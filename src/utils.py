


def to_snake_case(name: str) -> str:
    new_name = "_".join(name.lower().split(" "))
    return new_name