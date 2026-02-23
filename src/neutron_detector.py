"""Neutron radiation detection and monitoring system."""
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
import argparse
import csv


@dataclass
class DetectorUnit:
    """Represents a neutron detector unit."""
    id: str
    name: str
    location: str
    type: str  # he3_tube, boron_lined, scintillator, fission_chamber, activation_foil
    sensitivity: float
    baseline_cps: float
    status: str  # "online", "offline", "calibrating"
    last_calibration: str


@dataclass
class NeutronReading:
    """Represents a neutron detector reading."""
    detector_id: str
    cps: float  # counts per second
    dose_usv_h: float  # microsieverts per hour
    timestamp: str
    alert_triggered: bool


class NeutronDetectorNetwork:
    """Neutron detector network management system."""
    
    # CPS to dose conversion factors (rough estimates for different detector types)
    CPS_TO_DOSE = {
        "he3_tube": 0.0064,
        "boron_lined": 0.0042,
        "scintillator": 0.0055,
        "fission_chamber": 0.0075,
        "activation_foil": 0.0045
    }
    
    # Alert levels in μSv/h
    ALERT_LEVELS = {
        "normal": (0, 1),
        "elevated": (1, 10),
        "high": (10, 100),
        "critical": (100, float('inf'))
    }
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.path.expanduser("~/.blackroad/neutron.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS detectors (
                id TEXT PRIMARY KEY,
                name TEXT,
                location TEXT,
                type TEXT,
                sensitivity REAL,
                baseline_cps REAL,
                status TEXT,
                last_calibration TEXT,
                alert_threshold REAL
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                detector_id TEXT,
                cps REAL,
                dose_usv_h REAL,
                timestamp TEXT,
                alert_triggered BOOLEAN,
                FOREIGN KEY(detector_id) REFERENCES detectors(id)
            )
        """)
        
        conn.commit()
        conn.close()
    
    def _get_conn(self):
        """Get database connection."""
        return sqlite3.connect(self.db_path)
    
    def register_detector(self, name: str, location: str, detector_type: str,
                         sensitivity: float = 1.0) -> str:
        """Register a new detector unit."""
        import uuid
        detector_id = str(uuid.uuid4())[:8]
        now = datetime.now().isoformat()
        
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO detectors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (detector_id, name, location, detector_type, sensitivity, 
              0.0, "online", now, 100.0))  # default threshold 100 cps
        conn.commit()
        conn.close()
        
        return detector_id
    
    def record_reading(self, detector_id: str, cps: float) -> NeutronReading:
        """Record a reading from a detector."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT type, baseline_cps, alert_threshold FROM detectors WHERE id = ?",
                      (detector_id,))
        detector_row = cursor.fetchone()
        if not detector_row:
            conn.close()
            raise ValueError(f"Detector {detector_id} not found")
        
        detector_type, baseline, threshold = detector_row
        
        # Convert CPS to dose
        conversion_factor = self.CPS_TO_DOSE.get(detector_type, 0.005)
        dose_usv_h = cps * conversion_factor
        
        # Check if alert triggered
        alert_triggered = cps > threshold
        
        timestamp = datetime.now().isoformat()
        
        cursor.execute("""
            INSERT INTO readings (detector_id, cps, dose_usv_h, timestamp, alert_triggered)
            VALUES (?, ?, ?, ?, ?)
        """, (detector_id, cps, dose_usv_h, timestamp, alert_triggered))
        
        conn.commit()
        conn.close()
        
        return NeutronReading(detector_id, cps, dose_usv_h, timestamp, alert_triggered)
    
    def get_dose(self, detector_id: str, hours: int = 1) -> float:
        """Get integrated dose in μSv for past N hours."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        since = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        cursor.execute("""
            SELECT SUM(dose_usv_h) FROM readings
            WHERE detector_id = ? AND timestamp >= ?
        """, (detector_id, since))
        
        result = cursor.fetchone()[0]
        conn.close()
        
        return result if result else 0.0
    
    def set_threshold(self, detector_id: str, alert_cps: float) -> bool:
        """Set alert threshold for a detector."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("UPDATE detectors SET alert_threshold = ? WHERE id = ?",
                      (alert_cps, detector_id))
        conn.commit()
        conn.close()
        
        return True
    
    def fleet_status(self) -> List[Dict]:
        """Get status of all detectors."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, name, location, type, status FROM detectors")
        detectors = cursor.fetchall()
        
        status_list = []
        for detector in detectors:
            detector_id, name, location, detector_type, status = detector
            
            # Get latest reading
            cursor.execute("""
                SELECT cps, dose_usv_h, timestamp FROM readings
                WHERE detector_id = ? ORDER BY timestamp DESC LIMIT 1
            """, (detector_id,))
            
            reading = cursor.fetchone()
            if reading:
                cps, dose, ts = reading
                status_list.append({
                    "id": detector_id,
                    "name": name,
                    "location": location,
                    "type": detector_type,
                    "status": status,
                    "cps": cps,
                    "dose_usv_h": dose,
                    "timestamp": ts
                })
        
        conn.close()
        return status_list
    
    def anomaly_scan(self) -> List[Dict]:
        """Find detectors showing > 3x baseline activity."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, baseline_cps FROM detectors")
        detectors = cursor.fetchall()
        
        anomalies = []
        for detector_id, baseline in detectors:
            cursor.execute("""
                SELECT cps, timestamp FROM readings
                WHERE detector_id = ? ORDER BY timestamp DESC LIMIT 1
            """, (detector_id,))
            
            reading = cursor.fetchone()
            if reading:
                cps, ts = reading
                if cps > baseline * 3:
                    anomalies.append({
                        "detector_id": detector_id,
                        "baseline_cps": baseline,
                        "current_cps": cps,
                        "multiplier": round(cps / baseline, 2),
                        "timestamp": ts
                    })
        
        conn.close()
        return anomalies
    
    def get_spectrum(self, detector_id: str, hours: int = 24) -> List[Tuple]:
        """Get time-binned CPS readings for past N hours."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        since = (datetime.now() - timedelta(hours=hours)).isoformat()
        
        cursor.execute("""
            SELECT timestamp, cps FROM readings
            WHERE detector_id = ? AND timestamp >= ?
            ORDER BY timestamp
        """, (detector_id, since))
        
        readings = cursor.fetchall()
        conn.close()
        
        return readings
    
    def calibrate(self, detector_id: str) -> float:
        """Reset baseline to current 24h average."""
        spectrum = self.get_spectrum(detector_id, hours=24)
        
        if not spectrum:
            return 0.0
        
        avg_cps = sum(reading[1] for reading in spectrum) / len(spectrum)
        
        conn = self._get_conn()
        cursor = conn.cursor()
        
        now = datetime.now().isoformat()
        cursor.execute("""
            UPDATE detectors SET baseline_cps = ?, last_calibration = ?
            WHERE id = ?
        """, (avg_cps, now, detector_id))
        
        conn.commit()
        conn.close()
        
        return avg_cps
    
    def export_ndf(self, detector_id: str, output_path: str) -> bool:
        """Export detector data in Neutron Data Format (CSV with header)."""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT name, location, type, baseline_cps FROM detectors WHERE id = ?",
                      (detector_id,))
        detector = cursor.fetchone()
        if not detector:
            conn.close()
            return False
        
        cursor.execute("""
            SELECT timestamp, cps, dose_usv_h FROM readings
            WHERE detector_id = ? ORDER BY timestamp
        """, (detector_id,))
        
        readings = cursor.fetchall()
        conn.close()
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Detector", "Location", "Type", "Baseline_CPS"])
            writer.writerow([detector[0], detector[1], detector[2], detector[3]])
            writer.writerow([])
            writer.writerow(["Timestamp", "CPS", "Dose_uSv_h"])
            for reading in readings:
                writer.writerow(reading)
        
        return True


def cli():
    """Command-line interface."""
    parser = argparse.ArgumentParser(description="Neutron Detector Network")
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # fleet command
    fleet_parser = subparsers.add_parser("fleet", help="Show fleet status")
    
    # record command
    record_parser = subparsers.add_parser("record", help="Record reading")
    record_parser.add_argument("detector_id")
    record_parser.add_argument("cps", type=float)
    
    # anomalies command
    anomalies_parser = subparsers.add_parser("anomalies", help="Show anomalies")
    
    args = parser.parse_args()
    network = NeutronDetectorNetwork()
    
    if args.command == "fleet":
        status = network.fleet_status()
        for detector in status:
            print(f"{detector['id']} | {detector['name']} @ {detector['location']} | "
                  f"{detector['cps']:.1f} CPS | {detector['dose_usv_h']:.4f} μSv/h | "
                  f"{detector['status']}")
    
    elif args.command == "record":
        reading = network.record_reading(args.detector_id, args.cps)
        alert_str = " [ALERT]" if reading.alert_triggered else ""
        print(f"Recorded: {reading.cps:.1f} CPS → {reading.dose_usv_h:.4f} μSv/h{alert_str}")
    
    elif args.command == "anomalies":
        anomalies = network.anomaly_scan()
        if anomalies:
            for anomaly in anomalies:
                print(f"{anomaly['detector_id']} | {anomaly['current_cps']:.1f} CPS "
                      f"({anomaly['multiplier']}x baseline)")
        else:
            print("No anomalies detected")


if __name__ == "__main__":
    cli()
