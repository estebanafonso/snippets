import logging
import argparse
import sys
import psycopg2

# Set the log output file, and the log level
logging.basicConfig(filename="snippets.log", level=logging.DEBUG)

logging.debug("Connecting to PostgreSQL")
connection = psycopg2.connect("dbname='snippets'")
logging.debug("Database connection established.")

def put(name, snippet, hide, unhide):
    """Store a snippet with an associated name."""
    logging.info("Storing snippet {!r}: {!r}".format(name, snippet))
    try:
        with connection, connection.cursor() as cursor:
            command = "insert into snippets values (%s, %s)"
            cursor.execute(command, (name, snippet))
    except psycopg2.IntegrityError as e:
        with connection, connection.cursor() as cursor:
            command = "update snippets set message=%s where keyword=%s"
            cursor.execute(command, (snippet, name))
    logging.debug("Snippet stored successfully.")
    
    if hide:
        logging.info("Hiding {!r}: {!r}".format(name, snippet))

        with connection, connection.cursor() as cursor:
            command = "update snippets set hidden=TRUE where keyword=%s"
            cursor.execute(command, (name,))
            logging.debug("Snippet hidden successfully.")
            
            
    if unhide:
        logging.info("Unhiding {!r}: {!r}".format(name, snippet))

        with connection, connection.cursor() as cursor:
            command = "update snippets set hidden=FALSE where keyword=%s"
            cursor.execute(command, (name,))
            logging.debug("Snippet unhidden successfully.")

    return name, snippet
    
def get(name):
    """Retrieve the snippet with a given name.

    If there is no such snippet, return None.

    Returns the snippet.
    """
    logging.info("Fetching snippet {!r}".format(name))
    with connection, connection.cursor() as cursor:
        cursor.execute("select message from snippets where keyword=%s", (name,))
        row = cursor.fetchone()
    logging.debug("Snippet fetched successfully.")
    if not row:
        # No snippet was found with that name.
        return None
    return row[0]
    
def catalog():
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword from snippets where not hidden order by keyword ")
        keywords = cursor.fetchall()
    logging.debug("Keyword catalog fetched successfully.")
    return [tup[0] for tup in keywords]
    
def search(search_term):
    with connection, connection.cursor() as cursor:
        cursor.execute("select keyword, message from snippets where message like '%{}%' and not hidden".format(search_term))
        matches = cursor.fetchall()
    logging.debug("Matches fetched successfully.")
    return matches
    
def main():
    """Main function"""
    logging.info("Constructing parser")
    parser = argparse.ArgumentParser(description="Store and retrieve snippets of text")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Subparser for the put command
    logging.debug("Constructing put subparser")
    put_parser = subparsers.add_parser("put", help="Store a snippet")
    put_parser.add_argument("name", help="The name of the snippet")
    put_parser.add_argument("snippet", help="The snippet text")
    put_parser.add_argument("--hide", help="Set hidden flag to true", action="store_true")
    put_parser.add_argument("--unhide", help="Set hidden flag to false", action="store_true")
    
    logging.debug("Constructing get subparser")
    get_parser = subparsers.add_parser("get", help="Retrieve a snipper")
    get_parser.add_argument("name", help="The name of the snippet")
    
    logging.debug("Constructing catalog subparser")
    catalog_parser = subparsers.add_parser("catalog", help="Catalog keywords")
    
    logging.debug("Constructing search subparser")
    search_parser = subparsers.add_parser("search", help="Search snippet")
    search_parser.add_argument("search_term", help="The name of the search term")

    arguments = parser.parse_args(sys.argv[1:])
    # Convert parsed arguments from Namespace to dictionary
    arguments = vars(arguments)

    command = arguments.pop("command")

    if command == "put":
        name, snippet = put(**arguments)
        print("Stored {!r} as {!r}".format(snippet, name))
    elif command == "get":
        snippet = get(**arguments)
        print("Retrieved snippet: {!r}".format(snippet))
    elif command ==  "catalog":
        keywords_catalog = catalog()
        print("Keywords catalog: {!r}".format(keywords_catalog))
    elif command == "search":
        matches = search(**arguments)
        print("Snippets matched: {!r}".format(matches))
        

if __name__ == "__main__":
    main()