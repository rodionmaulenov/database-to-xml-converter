# ETL Pipeline

Hey! This is a simple tool that converts accounting data from a database into XML format.

## What it does

Takes journal entries from a SQLite database, cleans up the data (fixes dates, amounts, etc.), and outputs a nice XML file. Pretty straightforward!

The tool can handle messy data automatically - it converts different date formats (like 12/31/2024 or 31-12-2024) to standard format, fixes amounts with weird formatting (like "1,20" becomes "1.20"), validates account numbers, and skips any records that are completely broken. It also validates the final XML against a schema to make sure everything is correct.

## Quick Setup

1. Make sure you have Python 3.7+ installed
2. Clone this repo
3. Create a virtual environment:
   ```bash
   python3 -m venv venv
   ```
4. Activate the virtual environment:
   ```bash
   source venv/bin/activate  # On Linux/Mac
   venv\Scripts\activate     # On Windows
   ```
5. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## How to run it

**Basic usage:**
```bash
python solution.py
```

**With custom paths:**
```bash
python solution.py --db-path your_data.db --output-path results.xml
```

**See all options:**
```bash
python solution.py --help
```

**Custom settings:**
You can tweak settings like logging level and output formatting in `config.json`.

## What you'll get

The tool reads your database, processes the data, and creates an XML file like this:

```xml
<Journal>
  <Entry>
    <Date>2024-01-15</Date>
    <Account>1001</Account>
    <Amount>250.00</Amount>
    <Description>Office supplies</Description>
  </Entry>
</Journal>
```

You'll also see performance stats showing how fast it processed your data - like "Processed 30,000 records in 0.45s" with timing breakdown for each stage.

## Testing

Want to make sure everything works? Run the tests:

```bash
python -m pytest solution/test_solution.py -v
```

## If something goes wrong

- Make sure your database file exists
- Check that you have write permissions for the output location  
- The tool skips invalid records but keeps going, so don't worry if some data is messy

That's it! The tool handles most edge cases automatically and gives you detailed logs if you need to debug anything.