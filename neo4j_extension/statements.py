from .graph import Node, Relationship


def create_node(node: Node, label: str):
    query = f"""
    CREATE (n:{label})
    SET n += $props
    RETURN n
    """
