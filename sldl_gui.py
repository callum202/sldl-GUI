"""
sldl GUI — lightweight PyQt6 frontend for sldl.
Saves settings to sldl_gui_settings.json next to this script.
"""

import base64
import json
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from PyQt6.QtCore import QObject, Qt, QThread, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QFont, QIcon
from PyQt6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSizePolicy,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

SETTINGS_FILE = Path(__file__).parent / "sldl_gui_settings.json"

FORMATS = ["flac", "mp3", "ogg", "m4a", "opus", "wav", "aac", "alac"]

STATE_COLORS = {
    "Searching":    "#6ea8d4",
    "Downloading":  "#f0a830",
    "Downloaded":   "#5cb85c",
    "Failed":       "#d9534f",
    "AlreadyExists":"#888888",
    "NotFoundLastTime": "#aaaaaa",
    "Queued":       "#aaaaaa",
}


# ---------------------------------------------------------------------------
# Settings helpers
# ---------------------------------------------------------------------------

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            return json.loads(SETTINGS_FILE.read_text())
        except Exception:
            pass
    return {}


def save_settings(data: dict):
    SETTINGS_FILE.write_text(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Spotify playlist picker dialog
# ---------------------------------------------------------------------------

class SpotifyPlaylistDialog(QDialog):
    """Fetches the user's Spotify playlists and lets them pick one."""

    def __init__(self, parent, client_id: str, client_secret: str,
                 access_token: str, refresh_token: str):
        super().__init__(parent)
        self.setWindowTitle("Choose Spotify Playlist")
        self.resize(560, 420)
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._playlists: list[dict] = []
        self.selected_url: str | None = None
        # If we refreshed the token, expose it so the caller can save it.
        self.new_access_token: str | None = None
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)

        self._status = QLabel("Loading playlists…")
        layout.addWidget(self._status)

        self._list = QListWidget()
        self._list.itemDoubleClicked.connect(self._on_select)
        layout.addWidget(self._list, stretch=1)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton("Select")
        ok_btn.clicked.connect(self._on_select)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)

        QTimer.singleShot(0, self._load_playlists)

    # ------------------------------------------------------------------
    # Spotify API helpers (no external dependencies — stdlib only)
    # ------------------------------------------------------------------

    def _do_refresh(self) -> str:
        credentials = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode()
        ).decode()
        data = urllib.parse.urlencode({
            "grant_type": "refresh_token",
            "refresh_token": self._refresh_token,
        }).encode()
        req = urllib.request.Request(
            "https://accounts.spotify.com/api/token",
            data=data,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())["access_token"]

    def _fetch_playlists(self) -> list[dict]:
        playlists = []
        url: str | None = "https://api.spotify.com/v1/me/playlists?limit=50"
        while url:
            req = urllib.request.Request(
                url,
                headers={"Authorization": f"Bearer {self._access_token}"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            for item in data.get("items") or []:
                if item:
                    playlists.append({
                        "name":   item["name"],
                        "url":    item["external_urls"]["spotify"],
                        "tracks": item["tracks"]["total"],
                        "owner":  item["owner"]["display_name"],
                    })
            url = data.get("next")
        return playlists

    def _load_playlists(self):
        try:
            try:
                self._playlists = self._fetch_playlists()
            except urllib.error.HTTPError as e:
                if e.code == 401 and self._refresh_token and self._client_id and self._client_secret:
                    self._status.setText("Token expired — refreshing…")
                    self._access_token = self._do_refresh()
                    self.new_access_token = self._access_token
                    self._playlists = self._fetch_playlists()
                else:
                    raise

            self._list.clear()
            for p in self._playlists:
                self._list.addItem(
                    f"{p['name']}  —  {p['tracks']} tracks  ({p['owner']})"
                )
            n = len(self._playlists)
            self._status.setText(
                f"{n} playlist{'s' if n != 1 else ''} found. "
                "Double-click or select and press Select."
            )
        except Exception as exc:
            self._status.setText(f"Error: {exc}")

    def _on_select(self):
        row = self._list.currentRow()
        if row >= 0:
            self.selected_url = self._playlists[row]["url"]
            self.accept()


# ---------------------------------------------------------------------------
# Worker — runs sldl in a thread and emits signals
# ---------------------------------------------------------------------------

class SldlWorker(QObject):
    log_line      = pyqtSignal(str)           # raw stderr / non-JSON stdout
    sldl_event    = pyqtSignal(dict)          # parsed NDJSON event
    finished      = pyqtSignal(int)           # exit code

    def __init__(self, cmd: list[str]):
        super().__init__()
        self._cmd = cmd
        self._proc: subprocess.Popen | None = None

    def run(self):
        try:
            self._proc = subprocess.Popen(
                self._cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )

            # Read stderr in a background thread so it doesn't block stdout
            def read_stderr():
                for line in self._proc.stderr:
                    line = line.rstrip("\n")
                    if line:
                        self.log_line.emit(line)

            t = threading.Thread(target=read_stderr, daemon=True)
            t.start()

            for line in self._proc.stdout:
                line = line.rstrip("\n")
                if not line:
                    continue
                if line.startswith("{"):
                    try:
                        self.sldl_event.emit(json.loads(line))
                        continue
                    except json.JSONDecodeError:
                        pass
                self.log_line.emit(line)

            self._proc.wait()
            t.join(timeout=2)
            self.finished.emit(self._proc.returncode)
        except Exception as e:
            self.log_line.emit(f"[GUI error] {e}")
            self.finished.emit(-1)

    def stop(self):
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()


# ---------------------------------------------------------------------------
# Track table
# ---------------------------------------------------------------------------

COL_NUM    = 0
COL_ARTIST = 1
COL_TITLE  = 2
COL_STATUS = 3
COL_INFO   = 4


class TrackTable(QTableWidget):
    _KEY_ROLE = Qt.ItemDataRole.UserRole

    def __init__(self):
        super().__init__(0, 5)
        self.setHorizontalHeaderLabels(["#", "Artist", "Title", "Status", "Info"])
        hh = self.horizontalHeader()
        hh.setSectionResizeMode(COL_NUM,    QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(COL_ARTIST, QHeaderView.ResizeMode.Interactive)
        hh.setSectionResizeMode(COL_TITLE,  QHeaderView.ResizeMode.Stretch)
        hh.setSectionResizeMode(COL_STATUS, QHeaderView.ResizeMode.ResizeToContents)
        hh.setSectionResizeMode(COL_INFO,   QHeaderView.ResizeMode.Interactive)
        self.setColumnWidth(COL_ARTIST, 160)
        self.setColumnWidth(COL_INFO,   220)
        self.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        self.setAlternatingRowColors(True)
        self.verticalHeader().setDefaultSectionSize(22)
        self.setSortingEnabled(True)
        self.horizontalHeader().setSortIndicatorShown(True)

    def _key(self, artist: str, title: str) -> str:
        return f"{artist}|{title}"

    def _find_row(self, key: str) -> int | None:
        """Find a row by its stored key (works regardless of current sort order)."""
        for row in range(self.rowCount()):
            item = self.item(row, COL_NUM)
            if item and item.data(self._KEY_ROLE) == key:
                return row
        return None

    def populate(self, tracks: list[dict]):
        self.setSortingEnabled(False)
        self.setRowCount(0)
        for i, t in enumerate(tracks):
            self.insertRow(i)
            artist = t.get("artist") or ""
            title  = t.get("title")  or ""
            self._set_row(i, i + 1, artist, title, t.get("state", "Queued"), "")
        self.setSortingEnabled(True)

    def update_state(self, artist: str, title: str, status: str, info: str = ""):
        key = self._key(artist, title)
        row = self._find_row(key)
        if row is None:
            self.setSortingEnabled(False)
            row = self.rowCount()
            self.insertRow(row)
            self._set_row(row, row + 1, artist, title, status, info)
            self.setSortingEnabled(True)
        else:
            self._update_row(row, status, info)

    def _set_row(self, row, num, artist, title, status, info):
        key = self._key(artist, title)
        num_item = QTableWidgetItem(str(num))
        num_item.setData(self._KEY_ROLE, key)
        self.setItem(row, COL_NUM,    num_item)
        self.setItem(row, COL_ARTIST, QTableWidgetItem(artist))
        self.setItem(row, COL_TITLE,  QTableWidgetItem(title))
        status_item = QTableWidgetItem(status)
        color = STATE_COLORS.get(status)
        if color:
            status_item.setForeground(QColor(color))
        self.setItem(row, COL_STATUS, status_item)
        self.setItem(row, COL_INFO,   QTableWidgetItem(info))

    def _update_row(self, row, status, info):
        status_item = QTableWidgetItem(status)
        color = STATE_COLORS.get(status)
        if color:
            status_item.setForeground(QColor(color))
        self.setItem(row, COL_STATUS, status_item)
        if info:
            self.setItem(row, COL_INFO, QTableWidgetItem(info))


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("sldl")
        self.resize(900, 680)
        self._settings = load_settings()
        self._worker: SldlWorker | None = None
        self._thread: QThread | None = None
        self._downloaded = 0
        self._failed = 0
        self._total = 0

        self._build_ui()
        self._load_settings_into_ui()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)

        # ── Input row ──────────────────────────────────────────────────
        input_box = QGroupBox("Input")
        input_form = QFormLayout(input_box)
        input_form.setContentsMargins(8, 6, 8, 6)
        input_form.setSpacing(4)

        source_row = QHBoxLayout()
        self.input_edit = QLineEdit()
        self.input_edit.setPlaceholderText(
            "Spotify URL, YouTube URL, search string, or path to CSV / list file"
        )
        browse_csv_btn = QPushButton("Browse CSV…")
        browse_csv_btn.setFixedWidth(100)
        browse_csv_btn.setToolTip("Open an Exportify (or any sldl-compatible) CSV file")
        browse_csv_btn.clicked.connect(self._browse_csv)
        spotify_pl_btn = QPushButton("Spotify Playlists…")
        spotify_pl_btn.setFixedWidth(130)
        spotify_pl_btn.setToolTip(
            "Browse your Spotify playlists and select one as input.\n"
            "Requires Spotify credentials in the Credentials tab."
        )
        spotify_pl_btn.clicked.connect(self._pick_spotify_playlist)
        source_row.addWidget(self.input_edit)
        source_row.addWidget(browse_csv_btn)
        source_row.addWidget(spotify_pl_btn)
        input_form.addRow("Source:", source_row)
        layout.addWidget(input_box)

        # ── Tabs: Options / Credentials / Advanced ─────────────────────
        tabs = QTabWidget()
        layout.addWidget(tabs)

        tabs.addTab(self._build_options_tab(), "Options")
        tabs.addTab(self._build_credentials_tab(), "Credentials")
        tabs.addTab(self._build_advanced_tab(), "Advanced")

        # ── Controls ───────────────────────────────────────────────────
        ctrl_row = QHBoxLayout()
        self.download_btn = QPushButton("Download")
        self.download_btn.setFixedHeight(32)
        self.download_btn.clicked.connect(self._on_download)
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.setFixedHeight(32)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._on_stop)
        self.log_toggle_btn = QPushButton("Show log")
        self.log_toggle_btn.setFixedHeight(32)
        self.log_toggle_btn.setCheckable(True)
        self.log_toggle_btn.setChecked(False)
        self.log_toggle_btn.clicked.connect(self._toggle_log)
        ctrl_row.addWidget(self.download_btn)
        ctrl_row.addWidget(self.stop_btn)
        ctrl_row.addStretch()
        ctrl_row.addWidget(self.log_toggle_btn)
        layout.addLayout(ctrl_row)

        # ── Overall progress bar ───────────────────────────────────────
        self.overall_bar = QProgressBar()
        self.overall_bar.setTextVisible(True)
        self.overall_bar.setFormat("Ready")
        self.overall_bar.setValue(0)
        layout.addWidget(self.overall_bar)

        # ── Track table ────────────────────────────────────────────────
        self.track_table = TrackTable()
        layout.addWidget(self.track_table, stretch=1)

        # ── Log (hidden by default) ────────────────────────────────────
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        self.log_edit.setFont(QFont("Consolas", 8))
        self.log_edit.setFixedHeight(160)
        self.log_edit.setVisible(False)
        layout.addWidget(self.log_edit)

    def _build_options_tab(self) -> QWidget:
        w = QWidget()
        form = QFormLayout(w)
        form.setContentsMargins(8, 8, 8, 8)
        form.setSpacing(6)

        # Output path
        path_row = QHBoxLayout()
        self.path_edit = QLineEdit()
        self.path_edit.setPlaceholderText("Download directory")
        browse_btn = QPushButton("Browse…")
        browse_btn.setFixedWidth(72)
        browse_btn.clicked.connect(self._browse_output)
        path_row.addWidget(self.path_edit)
        path_row.addWidget(browse_btn)
        form.addRow("Output path:", path_row)

        # Format
        fmt_row = QHBoxLayout()
        self.pref_format_combo = QComboBox()
        self.pref_format_combo.addItem("(any)")
        for f in FORMATS:
            self.pref_format_combo.addItem(f)
        self.pref_format_combo.setToolTip("Preferred format — downloads this if available, falls back otherwise")

        self.strict_format_check = QCheckBox("Strict (reject other formats)")
        self.strict_format_check.setToolTip(
            "--format / --pref-format\n"
            "When checked, uses --format (fails if format not found).\n"
            "When unchecked, uses --pref-format (prefers but allows fallback)."
        )
        fmt_row.addWidget(self.pref_format_combo)
        fmt_row.addWidget(self.strict_format_check)
        fmt_row.addStretch()
        form.addRow("Format:", fmt_row)

        # Mode
        mode_row = QHBoxLayout()
        self.album_check = QCheckBox("Album mode")
        self.album_check.setToolTip("-a / --album\nDownload entire folders as albums")
        self.ytdlp_check = QCheckBox("yt-dlp fallback")
        self.ytdlp_check.setToolTip("--yt-dlp\nUse yt-dlp for tracks not found on Soulseek")
        mode_row.addWidget(self.album_check)
        mode_row.addWidget(self.ytdlp_check)
        mode_row.addStretch()
        form.addRow("Mode:", mode_row)

        # Concurrent downloads
        conc_row = QHBoxLayout()
        self.concurrent_combo = QComboBox()
        for n in ["1", "2", "3", "4"]:
            self.concurrent_combo.addItem(n)
        self.concurrent_combo.setCurrentText("2")
        self.concurrent_combo.setFixedWidth(60)
        conc_row.addWidget(self.concurrent_combo)
        conc_row.addStretch()
        form.addRow("Concurrent downloads:", conc_row)

        # Number / offset
        num_off_row = QHBoxLayout()
        self.number_edit = QLineEdit()
        self.number_edit.setPlaceholderText("All")
        self.number_edit.setFixedWidth(70)
        self.number_edit.setToolTip("--number\nStop after this many tracks")
        self.offset_edit = QLineEdit()
        self.offset_edit.setPlaceholderText("0")
        self.offset_edit.setFixedWidth(70)
        self.offset_edit.setToolTip("--offset\nSkip this many tracks from the start")
        num_off_row.addWidget(QLabel("Number:"))
        num_off_row.addWidget(self.number_edit)
        num_off_row.addSpacing(12)
        num_off_row.addWidget(QLabel("Offset:"))
        num_off_row.addWidget(self.offset_edit)
        num_off_row.addStretch()
        form.addRow("Limits:", num_off_row)

        # Skip existing
        self.no_skip_existing_check = QCheckBox("Don't skip already-downloaded tracks")
        self.no_skip_existing_check.setToolTip(
            "--no-skip-existing\n"
            "By default sldl skips tracks it finds in the index or output folder.\n"
            "Check this to force re-downloading everything."
        )
        form.addRow("", self.no_skip_existing_check)

        return w

    def _build_credentials_tab(self) -> QWidget:
        w = QWidget()
        form = QFormLayout(w)
        form.setContentsMargins(8, 8, 8, 8)
        form.setSpacing(6)

        self.slsk_user_edit = QLineEdit()
        self.slsk_user_edit.setPlaceholderText("Soulseek username")
        form.addRow("Soulseek username:", self.slsk_user_edit)

        self.slsk_pass_edit = QLineEdit()
        self.slsk_pass_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.slsk_pass_edit.setPlaceholderText("Soulseek password")
        form.addRow("Soulseek password:", self.slsk_pass_edit)

        form.addRow(QLabel(""))  # spacer
        form.addRow(QLabel("Spotify (optional — only needed for Spotify sources):"))

        self.spotify_id_edit = QLineEdit()
        self.spotify_id_edit.setPlaceholderText("Client ID")
        form.addRow("Spotify client ID:", self.spotify_id_edit)

        self.spotify_secret_edit = QLineEdit()
        self.spotify_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.spotify_secret_edit.setPlaceholderText("Client secret")
        form.addRow("Spotify client secret:", self.spotify_secret_edit)

        self.spotify_token_edit = QLineEdit()
        self.spotify_token_edit.setPlaceholderText("Access token (auto-filled after first OAuth login)")
        form.addRow("Spotify token:", self.spotify_token_edit)

        self.spotify_refresh_edit = QLineEdit()
        self.spotify_refresh_edit.setPlaceholderText("Refresh token (auto-filled after first OAuth login)")
        form.addRow("Spotify refresh:", self.spotify_refresh_edit)

        return w

    def _build_advanced_tab(self) -> QWidget:
        w = QWidget()
        form = QFormLayout(w)
        form.setContentsMargins(8, 8, 8, 8)
        form.setSpacing(6)

        # sldl executable path
        sldl_row = QHBoxLayout()
        self.sldl_path_edit = QLineEdit()
        self.sldl_path_edit.setPlaceholderText("Path to sldl executable (e.g. C:\\tools\\sldl.exe)")
        browse_sldl = QPushButton("Browse…")
        browse_sldl.setFixedWidth(72)
        browse_sldl.clicked.connect(self._browse_sldl)
        sldl_row.addWidget(self.sldl_path_edit)
        sldl_row.addWidget(browse_sldl)
        form.addRow("sldl executable:", sldl_row)

        # Min bitrate
        self.min_bitrate_edit = QLineEdit()
        self.min_bitrate_edit.setPlaceholderText("e.g. 192  (leave blank for no limit)")
        self.min_bitrate_edit.setFixedWidth(120)
        form.addRow("Min bitrate (kbps):", self.min_bitrate_edit)

        # Extra flags
        self.extra_args_edit = QLineEdit()
        self.extra_args_edit.setPlaceholderText("e.g. --desperate --remove-ft")
        form.addRow("Extra flags:", self.extra_args_edit)

        # Show command preview
        preview_btn = QPushButton("Preview command…")
        preview_btn.clicked.connect(self._preview_command)
        form.addRow("", preview_btn)

        return w

    # ------------------------------------------------------------------
    # Settings persistence
    # ------------------------------------------------------------------

    def _load_settings_into_ui(self):
        s = self._settings
        self.input_edit.setText(s.get("input", ""))
        self.path_edit.setText(s.get("output_path", ""))
        self.slsk_user_edit.setText(s.get("slsk_user", ""))
        self.slsk_pass_edit.setText(s.get("slsk_pass", ""))
        self.spotify_id_edit.setText(s.get("spotify_id", ""))
        self.spotify_secret_edit.setText(s.get("spotify_secret", ""))
        self.spotify_token_edit.setText(s.get("spotify_token", ""))
        self.spotify_refresh_edit.setText(s.get("spotify_refresh", ""))
        self.sldl_path_edit.setText(s.get("sldl_path", "sldl"))
        self.min_bitrate_edit.setText(s.get("min_bitrate", ""))
        self.extra_args_edit.setText(s.get("extra_args", ""))
        self.album_check.setChecked(s.get("album_mode", False))
        self.ytdlp_check.setChecked(s.get("ytdlp", False))
        self.strict_format_check.setChecked(s.get("strict_format", False))
        self.concurrent_combo.setCurrentText(str(s.get("concurrent", "2")))
        self.number_edit.setText(s.get("number", ""))
        self.offset_edit.setText(s.get("offset", ""))
        self.no_skip_existing_check.setChecked(s.get("no_skip_existing", False))

        fmt = s.get("format", "")
        idx = self.pref_format_combo.findText(fmt)
        self.pref_format_combo.setCurrentIndex(max(idx, 0))

    def _collect_settings(self) -> dict:
        return {
            "input":           self.input_edit.text().strip(),
            "output_path":     self.path_edit.text().strip(),
            "slsk_user":       self.slsk_user_edit.text().strip(),
            "slsk_pass":       self.slsk_pass_edit.text(),
            "spotify_id":      self.spotify_id_edit.text().strip(),
            "spotify_secret":  self.spotify_secret_edit.text(),
            "spotify_token":   self.spotify_token_edit.text().strip(),
            "spotify_refresh": self.spotify_refresh_edit.text().strip(),
            "sldl_path":       self.sldl_path_edit.text().strip() or "sldl",
            "format":          self.pref_format_combo.currentText() if self.pref_format_combo.currentIndex() > 0 else "",
            "strict_format":   self.strict_format_check.isChecked(),
            "album_mode":      self.album_check.isChecked(),
            "ytdlp":           self.ytdlp_check.isChecked(),
            "concurrent":        self.concurrent_combo.currentText(),
            "number":            self.number_edit.text().strip(),
            "offset":            self.offset_edit.text().strip(),
            "no_skip_existing":  self.no_skip_existing_check.isChecked(),
            "min_bitrate":       self.min_bitrate_edit.text().strip(),
            "extra_args":      self.extra_args_edit.text().strip(),
        }

    def closeEvent(self, event):
        save_settings(self._collect_settings())
        super().closeEvent(event)

    # ------------------------------------------------------------------
    # Command building
    # ------------------------------------------------------------------

    def _build_command(self, s: dict) -> list[str] | None:
        exe = s.get("sldl_path") or "sldl"
        inp = s.get("input", "").strip()
        if not inp:
            QMessageBox.warning(self, "sldl", "Please enter an input (URL, search string, or file path).")
            return None

        cmd = [exe, inp, "--progress-json"]

        if s.get("slsk_user"):
            cmd += ["--username", s["slsk_user"]]
        if s.get("slsk_pass"):
            cmd += ["--password", s["slsk_pass"]]
        if s.get("output_path"):
            cmd += ["--path", s["output_path"]]

        fmt = s.get("format", "")
        if fmt:
            flag = "--format" if s.get("strict_format") else "--pref-format"
            cmd += [flag, fmt]

        if s.get("album_mode"):
            cmd.append("--album")
        if s.get("ytdlp"):
            cmd.append("--yt-dlp")
        if s.get("number"):
            cmd += ["--number", s["number"]]
        if s.get("offset"):
            cmd += ["--offset", s["offset"]]
        if s.get("no_skip_existing"):
            cmd.append("--no-skip-existing")

        conc = s.get("concurrent", "2")
        if conc != "2":
            cmd += ["--concurrent-downloads", conc]

        if s.get("min_bitrate"):
            cmd += ["--pref-min-bitrate", s["min_bitrate"]]

        if s.get("spotify_id"):
            cmd += ["--spotify-id", s["spotify_id"]]
        if s.get("spotify_secret"):
            cmd += ["--spotify-secret", s["spotify_secret"]]
        if s.get("spotify_token"):
            cmd += ["--spotify-token", s["spotify_token"]]
        if s.get("spotify_refresh"):
            cmd += ["--spotify-refresh", s["spotify_refresh"]]

        extra = s.get("extra_args", "").strip()
        if extra:
            import shlex
            cmd += shlex.split(extra)

        return cmd

    # ------------------------------------------------------------------
    # Slots
    # ------------------------------------------------------------------

    def _browse_csv(self):
        f, _ = QFileDialog.getOpenFileName(
            self, "Open playlist CSV", "", "CSV files (*.csv);;All files (*)"
        )
        if not f:
            return
        self.input_edit.setText(f)
        # Exportify exports duration in milliseconds; detect that and add --time-unit ms
        # so sldl doesn't interpret 179000 as 179 000 seconds.
        self._apply_exportify_time_unit(f)

    def _apply_exportify_time_unit(self, csv_path: str):
        """If the CSV has a '(ms)' duration column, inject --time-unit ms."""
        try:
            with open(csv_path, encoding="utf-8-sig", errors="replace") as fh:
                header = fh.readline().lower()
        except OSError:
            return
        if "(ms)" not in header:
            return
        extra = self.extra_args_edit.text()
        if "--time-format" not in extra:
            sep = " " if extra else ""
            self.extra_args_edit.setText(extra + sep + "--time-format ms")

    def _pick_spotify_playlist(self):
        s = self._collect_settings()
        token   = s.get("spotify_token", "").strip()
        refresh = s.get("spotify_refresh", "").strip()
        cid     = s.get("spotify_id", "").strip()
        secret  = s.get("spotify_secret", "").strip()

        if not token and not refresh:
            QMessageBox.information(
                self,
                "Spotify credentials needed",
                "Enter your Spotify client ID, client secret, and access/refresh token "
                "in the Credentials tab, then try again.\n\n"
                "Tip: run sldl once with a Spotify URL to complete the OAuth flow and "
                "auto-populate the token fields.",
            )
            return

        dlg = SpotifyPlaylistDialog(self, cid, secret, token, refresh)
        if dlg.exec() == QDialog.DialogCode.Accepted and dlg.selected_url:
            self.input_edit.setText(dlg.selected_url)
            # Persist a refreshed token if we had to renew it
            if dlg.new_access_token:
                self.spotify_token_edit.setText(dlg.new_access_token)
                save_settings(self._collect_settings())

    def _browse_output(self):
        d = QFileDialog.getExistingDirectory(self, "Select download folder", self.path_edit.text())
        if d:
            self.path_edit.setText(d)

    def _browse_sldl(self):
        f, _ = QFileDialog.getOpenFileName(self, "Locate sldl executable", "", "Executables (*.exe);;All files (*)")
        if f:
            self.sldl_path_edit.setText(f)

    def _preview_command(self):
        s = self._collect_settings()
        cmd = self._build_command(s)
        if cmd:
            QMessageBox.information(self, "Command preview", " ".join(
                f'"{a}"' if " " in a else a for a in cmd
            ))

    def _on_download(self):
        s = self._collect_settings()
        save_settings(s)
        cmd = self._build_command(s)
        if not cmd:
            return

        # Reset UI
        self.track_table.setRowCount(0)
        self.log_edit.clear()
        self._downloaded = 0
        self._failed = 0
        self._total = 0
        self.overall_bar.setValue(0)
        self.overall_bar.setFormat("Starting…")
        self.download_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)

        self._log(f"$ {' '.join(cmd)}")

        self._thread = QThread()
        self._worker = SldlWorker(cmd)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.log_line.connect(self._log)
        self._worker.sldl_event.connect(self._handle_event)
        self._worker.finished.connect(self._on_finished)
        self._worker.finished.connect(self._thread.quit)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _toggle_log(self, checked: bool):
        self.log_edit.setVisible(checked)
        self.log_toggle_btn.setText("Hide log" if checked else "Show log")

    def _on_stop(self):
        if self._worker:
            self._worker.stop()
        self.stop_btn.setEnabled(False)

    def _on_finished(self, code: int):
        self.download_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        if code == 0:
            self.overall_bar.setFormat(
                f"Done — {self._downloaded} downloaded, {self._failed} failed"
            )
            self._log(f"[finished] exit code {code}")
        else:
            self.overall_bar.setFormat(f"Stopped (exit {code})")
            self._log(f"[finished] exit code {code}")

    # ------------------------------------------------------------------
    # Event handling (NDJSON from --progress-json)
    # ------------------------------------------------------------------

    def _handle_event(self, ev: dict):
        t = ev.get("type")
        d = ev.get("data", {})

        if t == "track_list":
            self.track_table.populate(d.get("tracks", []))
            self._total = d.get("total", 0)
            self._update_bar()

        elif t == "search_start":
            self.track_table.update_state(
                d.get("artist", ""), d.get("title", ""), "Searching"
            )

        elif t == "search_result":
            n = d.get("resultCount", 0)
            cf = d.get("chosenFile")
            info = ""
            if cf:
                parts = []
                if cf.get("extension"):
                    parts.append(cf["extension"].upper())
                if cf.get("bitRate"):
                    parts.append(f"{cf['bitRate']} kbps")
                if cf.get("sampleRate"):
                    parts.append(f"{cf['sampleRate'] // 1000} kHz")
                if parts:
                    info = " · ".join(parts)
                    info += f"  ({n} results)"
            else:
                info = f"{n} results"
            status = "Searching" if n > 0 else "Failed"
            self.track_table.update_state(d.get("artist", ""), d.get("title", ""), status, info)

        elif t == "download_start":
            ext = (d.get("extension") or "").upper()
            size_mb = (d.get("size") or 0) / 1_048_576
            info = f"{ext}  {size_mb:.1f} MB  ←  {d.get('username', '')}"
            self.track_table.update_state(d.get("artist", ""), d.get("title", ""), "Downloading", info)

        elif t == "download_progress":
            pct = d.get("percent", 0)
            artist, title = d.get("artist", ""), d.get("title", "")
            key = self.track_table._key(artist, title)
            row = self.track_table._find_row(key)
            if row is not None:
                info_item = self.track_table.item(row, COL_INFO)
                base = (info_item.text().split("  ")[0]) if info_item else ""
                self.track_table.setItem(
                    row, COL_INFO, QTableWidgetItem(f"{base}  {pct:.0f}%")
                )

        elif t == "track_state":
            state = d.get("state", "")
            ext = (d.get("extension") or "").upper()
            br  = d.get("bitRate")
            info_parts = []
            if ext:
                info_parts.append(ext)
            if br:
                info_parts.append(f"{br} kbps")
            if d.get("username"):
                info_parts.append(d["username"])
            if d.get("failureReason"):
                info_parts.append(d["failureReason"])
            self.track_table.update_state(
                d.get("artist", ""), d.get("title", ""),
                state, "  ".join(info_parts)
            )

        elif t == "progress":
            self._downloaded = d.get("downloaded", self._downloaded)
            self._failed     = d.get("failed",     self._failed)
            self._total      = d.get("total",       self._total)
            self._update_bar()

        elif t == "job_complete":
            self._downloaded = d.get("downloaded", self._downloaded)
            self._failed     = d.get("failed",     self._failed)
            self._total      = d.get("total",       self._total)
            self._update_bar()

    def _update_bar(self):
        if self._total <= 0:
            return
        done = self._downloaded + self._failed
        self.overall_bar.setMaximum(self._total)
        self.overall_bar.setValue(done)
        remaining = self._total - done
        self.overall_bar.setFormat(
            f"{self._downloaded} downloaded · {self._failed} failed · {remaining} remaining · {self._total} total"
        )

    # ------------------------------------------------------------------
    # Log
    # ------------------------------------------------------------------

    def _log(self, text: str):
        # Intercept spotify-token / spotify-refresh lines and store them
        for prefix in ("spotify-token=", "spotify-refresh="):
            if text.startswith(prefix):
                value = text[len(prefix):].strip()
                if prefix == "spotify-token=":
                    self.spotify_token_edit.setText(value)
                    self._settings["spotify_token"] = value
                    save_settings(self._collect_settings())
                else:
                    self.spotify_refresh_edit.setText(value)
                    self._settings["spotify_refresh"] = value
                    save_settings(self._collect_settings())

        self.log_edit.append(text)
        # Auto-scroll
        sb = self.log_edit.verticalScrollBar()
        sb.setValue(sb.maximum())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
