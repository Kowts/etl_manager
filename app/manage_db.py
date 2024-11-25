import click
import subprocess
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

@click.group()
def cli():
    """Database management commands"""
    pass

@cli.command()
def backup():
    """Backup PostgreSQL database"""
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_file = f"backup_{db_name}_{timestamp}.sql"

    try:
        subprocess.run([
            'pg_dump',
            '-U', db_user,
            '-d', db_name,
            '-f', backup_file
        ], check=True)
        click.echo(f"Database backed up to {backup_file}")
    except subprocess.CalledProcessError as e:
        click.echo(f"Backup failed: {str(e)}")

@cli.command()
@click.argument('backup_file')
def restore(backup_file):
    """Restore PostgreSQL database from backup"""
    db_name = os.getenv('DB_NAME')
    db_user = os.getenv('DB_USER')

    try:
        # Drop existing connections
        subprocess.run([
            'psql',
            '-U', db_user,
            '-d', 'postgres',
            '-c', f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}'"
        ], check=True)

        # Drop and recreate database
        subprocess.run([
            'psql',
            '-U', db_user,
            '-d', 'postgres',
            '-c', f"DROP DATABASE IF EXISTS {db_name}"
        ], check=True)

        subprocess.run([
            'psql',
            '-U', db_user,
            '-d', 'postgres',
            '-c', f"CREATE DATABASE {db_name}"
        ], check=True)

        # Restore from backup
        subprocess.run([
            'psql',
            '-U', db_user,
            '-d', db_name,
            '-f', backup_file
        ], check=True)

        click.echo("Database restored successfully!")
    except subprocess.CalledProcessError as e:
        click.echo(f"Restore failed: {str(e)}")

@cli.command()
def clear():
    """Clear all data from the database"""
    if click.confirm('Are you sure you want to clear all data? This cannot be undone!'):
        from database_setup import db_config
        from models import Base

        Base.metadata.drop_all(db_config.engine)
        Base.metadata.create_all(db_config.engine)
        click.echo("Database cleared successfully!")

if __name__ == '__main__':
    cli()
