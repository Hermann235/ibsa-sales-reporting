"""Cancella i database SQLite per ripartire la demo da zero (i CSV di landing restano)."""
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def main():
    removed = []
    for db_file in DATA_DIR.glob("*.db"):
        db_file.unlink()
        removed.append(db_file.name)
    print("rimossi: " + ", ".join(removed) if removed else "nessun database da rimuovere")


if __name__ == "__main__":
    main()
