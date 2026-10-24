"""CLI commands for b2backup."""
import os
import sys
from pathlib import Path
import click
from tqdm import tqdm
from .config import load_config
from .storage import B2AccountPool, BucketValidationError, ConsistencyError


def _get_pool(ctx):
    """Lazily create pool on first command that needs it."""
    if "pool" not in ctx.obj:
        config_path = ctx.obj.get("config")
        if not config_path:
            raise click.UsageError("Missing --config")
        try:
            config = load_config(str(config_path))
        except ValueError as e:
            click.echo(f"ERROR: Invalid config: {e}", err=True)
            sys.exit(1)
        try:
            ctx.obj["pool"] = B2AccountPool(config.accounts, config.max_files_per_account)
        except BucketValidationError as e:
            click.echo(str(e), err=True)
            sys.exit(1)
        except ConsistencyError as e:
            click.echo(str(e), err=True)
            sys.exit(2)
    return ctx.obj["pool"]


@click.group()
@click.option(
    "--config", "-c",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to accounts.yaml",
    envvar="B2BACKUP_CONFIG",
)
@click.pass_context
def cli(ctx, config: Path):
    """b2backup - Personal backup to Backblaze B2 multi-account pool."""
    ctx.ensure_object(dict)
    ctx.obj["config"] = config


@cli.command()
@click.argument(
    "source",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.pass_context
def backup(ctx, source: Path):
    """Backup a directory.

    Files are stored with relative path from source's parent.
    Example: backup /ABC/CDE/EFG -> files stored as EFG/HIJ/KLM/file.ext
    """
    pool: B2AccountPool = _get_pool(ctx)
    source = source.resolve()

    # Backup root = last component of source path
    # e.g., /ABC/CDE/EFG -> root = "EFG"
    root_name = source.name

    files = list(source.rglob("*"))
    files = [f for f in files if f.is_file()]

    if not files:
        click.echo("No files to backup")
        return

    click.echo(f"Backing up {len(files)} files from {source} (root: {root_name}/)")

    for f in tqdm(files, desc="Uploading", unit="file"):
        # Relative path from source parent: EFG/HIJ/KLM/file.ext
        rel = f.relative_to(source.parent)
        pool.upload_file(str(rel), f)

    click.echo("Backup complete")


@cli.command()
@click.argument("filename")
@click.argument("dest", type=click.Path())
@click.pass_context
def restore(ctx, filename: str, dest: str):
    """Restore a file by its stored name (e.g., EFG/HIJ/KLM/file.ext)."""
    pool: B2AccountPool = _get_pool(ctx)
    dest_path = Path(dest).expanduser()

    # Treat an existing directory or a path ending in a separator as a directory.
    dir_separators = {"/", os.sep}
    if os.altsep:
        dir_separators.add(os.altsep)
    if dest_path.is_dir() or any(dest.endswith(sep) for sep in dir_separators):
        dest_path = dest_path / Path(filename).name

    dest_path = dest_path.resolve()

    click.echo(f"Restoring {filename} -> {dest_path}")
    pool.download_file(filename, dest_path)
    click.echo("Done")


@cli.command(name="ls")
@click.pass_context
def list_files(ctx):
    """List all backed up files."""
    pool: B2AccountPool = _get_pool(ctx)
    files = pool.list_files()

    if not files:
        click.echo("No files")
        return

    # Header
    click.echo(f"{'Name':<60} {'Size':>10} {'Type':<20} {'Account'}")
    click.echo("-" * 100)

    for f in files:
        size_mb = f["size"] / (1024 * 1024)
        name = f["name"]
        if len(name) > 58:
            name = "..." + name[-55:]
        click.echo(f"{name:<60} {size_mb:>9.2f}MB {f['content_type']:<20} {f['account']}")


@cli.command()
@click.pass_context
def stats(ctx):
    """Show pool capacity and usage."""
    pool: B2AccountPool = _get_pool(ctx)
    data = pool.get_pool_stats()

    click.echo(f"Accounts: {data['accounts']}")
    click.echo(f"Total files: {data['total_files']}")
    click.echo(f"Total size: {data['total_bytes'] / 1024 / 1024 / 1024:.2f} GB")
    click.echo()

    click.echo(f"{'Account':<10} {'Bucket':<20} {'Files':>6} {'Used':>10} {'Rem Files':>10} {'Rem Space':>12}")
    click.echo("-" * 80)
    for a in data['per_account']:
        click.echo(
            f"{a['account_id']:<10} {a['bucket']:<20} "
            f"{a['files']:>6} {a['used_mb']:>9.1f}MB "
            f"{a['remaining_files']:>10} {str(a['remaining_mb']):>12}"
        )


def main():
    cli(obj={})


if __name__ == "__main__":
    main()