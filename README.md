# arc-db

Database operations for the Arc toolkit.

## Features

- **info** - Show database info and table counts
- **migrate** - Run database migrations
- **vacuum** - Optimize database
- **export** - Export database contents
- **path** - Show database file path

## Installation

```bash
go install github.com/mtreilly/arc-db@latest
```

## Usage

```bash
# Show database info
arc-db info

# Run migrations
arc-db migrate

# Vacuum the database
arc-db vacuum

# Export data
arc-db export --format json

# Show database path
arc-db path
```

## License

MIT
