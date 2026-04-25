from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from my_bt_lab.app import mt4_desktop


class Mt4DesktopStartupTests(unittest.TestCase):
    def test_startup_template_override_returns_none_when_unset(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(mt4_desktop.startup_template_override())

    def test_startup_template_override_resolves_relative_path_from_project_root(self):
        rel_path = "my_bt_lab/app/configs/quant_lab_aliyun_ssh.yaml"
        with patch.dict(os.environ, {"MY_BT_LAB_DEFAULT_TEMPLATE": rel_path}, clear=True):
            resolved = mt4_desktop.startup_template_override()
        self.assertIsNotNone(resolved)
        self.assertEqual(resolved, (mt4_desktop.project_root() / rel_path).resolve())

    def test_startup_template_override_preserves_absolute_path(self):
        abs_path = str((mt4_desktop.project_root() / "my_bt_lab/app/configs/quant_lab_aliyun_ssh.yaml").resolve())
        with patch.dict(os.environ, {"MY_BT_LAB_DEFAULT_TEMPLATE": abs_path}, clear=True):
            resolved = mt4_desktop.startup_template_override()
        self.assertEqual(resolved, Path(abs_path))


if __name__ == "__main__":
    unittest.main()
