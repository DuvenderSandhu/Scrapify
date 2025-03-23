from switchScript import switch_scrapers

# Example usage
fields = ['name', 'email']  # Only scrape name and email
scripts = ['script1', 'script2']  # Your script names
total_agents = switch_scrapers(fields=fields, scripts=scripts, resume=True)
print(f"Total agents scraped: {total_agents}")