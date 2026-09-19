import json
import tempfile
import unittest
from pathlib import Path

from cloud_state_sync import durable_state_paths, export_to_directory


class CloudStateSyncTests(unittest.TestCase):
    def test_manifest_selects_all_five_json_states_and_excludes_lock(self):
        paths = set(durable_state_paths())
        self.assertEqual(
            paths,
            {
                Path("state/radar_state.json"),
                Path("state/technical_state.json"),
                Path("state/divergence_state.json"),
                Path("state/heartbeat.json"),
                Path("state/run_status.json"),
            },
        )
        self.assertNotIn(Path("state/pipeline.lock"), paths)

    def test_export_copies_valid_durable_state_only(self):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
            source = Path(source_dir)
            target = Path(target_dir)
            for path in durable_state_paths():
                destination = source / path
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_text(json.dumps({"file": str(path)}), encoding="utf-8")
            lock = source / "state" / "pipeline.lock"
            lock.write_text("locked", encoding="utf-8")

            exported = export_to_directory(target, source)

            self.assertEqual(set(exported), set(durable_state_paths()))
            for path in exported:
                self.assertEqual(json.loads((target / path).read_text(encoding="utf-8"))["file"], str(path))
            self.assertFalse((target / "state" / "pipeline.lock").exists())

    def test_export_fails_when_required_state_is_missing(self):
        with tempfile.TemporaryDirectory() as source_dir, tempfile.TemporaryDirectory() as target_dir:
            with self.assertRaisesRegex(RuntimeError, "required durable state is missing"):
                export_to_directory(Path(target_dir), Path(source_dir))


if __name__ == "__main__":
    unittest.main()
