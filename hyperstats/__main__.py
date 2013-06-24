import sys, logging

logging.basicConfig()

def main(args):
    if len(args) == 1:
        print "Usage: python -mhyperstats <module> [options]"
        return 1
    module = args[1]
    main = None
    if module == "aggregator":
        from hyperstats.aggregator import main
    elif module == "httpd":
        from hyperstats.httpd import main
    if main is None:
        print "Error: unknown module '%s'" % (module,)
        return 2
    main(args[2:])
    return 0

if __name__ == "__main__":
	sys.exit(main(sys.argv))
