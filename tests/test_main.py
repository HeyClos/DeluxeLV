"""
Property-based tests for Main ETL Script and Execution Control.

Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
Validates: Requirements 5.2
"""

import os
import tempfile
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

import pytest
from hypothesis import given, strategies as st, settings, assume

from trestle_etl.main import ExecutionLock, LockAcquisitionError


class TestExecutionLockPrevention:
    """
    Property 16: Execution Lock Prevention
    
    For any attempt to run overlapping ETL executions, only the first instance 
    should proceed while subsequent attempts should exit gracefully.
    
    Validates: Requirements 5.2
    """
    
    def setup_method(self):
        """Create a temporary directory for lock files."""
        self.temp_dir = tempfile.mkdtemp()
    
    def teardown_method(self):
        """Clean up temporary directory."""
        import shutil
        try:
            shutil.rmtree(self.temp_dir)
        except OSError:
            pass
    
    def get_lock_path(self, name: str = "test") -> str:
        """Get a unique lock file path."""
        return os.path.join(self.temp_dir, f"{name}.lock")
    
    @given(
        num_concurrent=st.integers(min_value=2, max_value=10)
    )
    @settings(max_examples=20)
    def test_only_first_instance_acquires_lock(self, num_concurrent: int):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        For any number of concurrent lock acquisition attempts, exactly one
        should succeed and all others should fail gracefully.
        """
        lock_path = self.get_lock_path(f"concurrent_{num_concurrent}")
        results: List[Tuple[int, bool]] = []
        results_lock = threading.Lock()
        
        def try_acquire_lock(thread_id: int) -> Tuple[int, bool]:
            """Attempt to acquire lock and return result."""
            lock = ExecutionLock(lock_path)
            acquired = lock.acquire()
            
            with results_lock:
                results.append((thread_id, acquired))
            
            # Hold lock briefly if acquired
            if acquired:
                time.sleep(0.1)
                lock.release()
            
            return thread_id, acquired
        
        # Run concurrent acquisition attempts
        with ThreadPoolExecutor(max_workers=num_concurrent) as executor:
            futures = [
                executor.submit(try_acquire_lock, i) 
                for i in range(num_concurrent)
            ]
            
            # Wait for all to complete
            for future in as_completed(futures):
                future.result()
        
        # Verify exactly one succeeded
        successful = [r for r in results if r[1]]
        failed = [r for r in results if not r[1]]
        
        assert len(successful) == 1, f"Expected exactly 1 success, got {len(successful)}"
        assert len(failed) == num_concurrent - 1, f"Expected {num_concurrent - 1} failures"
    
    @given(
        lock_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
            min_size=1,
            max_size=20
        )
    )
    @settings(max_examples=20)
    def test_lock_acquire_and_release_cycle(self, lock_name: str):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        For any lock, acquiring and releasing should allow subsequent acquisitions.
        """
        lock_path = self.get_lock_path(lock_name)
        
        # First acquisition
        lock1 = ExecutionLock(lock_path)
        assert lock1.acquire() is True
        assert lock1.is_locked() is True
        
        # Second attempt should fail while first holds lock
        lock2 = ExecutionLock(lock_path)
        assert lock2.acquire() is False
        
        # Release first lock
        lock1.release()
        assert lock1.is_locked() is False
        
        # Now second should succeed
        assert lock2.acquire() is True
        assert lock2.is_locked() is True
        
        # Cleanup
        lock2.release()
    
    @given(
        lock_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
            min_size=1,
            max_size=20
        )
    )
    @settings(max_examples=20)
    def test_context_manager_releases_lock(self, lock_name: str):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        For any lock used as context manager, exiting the context should release
        the lock allowing subsequent acquisitions.
        """
        lock_path = self.get_lock_path(lock_name)
        
        # Use context manager
        with ExecutionLock(lock_path) as lock:
            assert lock.is_locked() is True
            
            # Another lock should fail
            other_lock = ExecutionLock(lock_path)
            assert other_lock.acquire() is False
        
        # After context exit, new lock should succeed
        new_lock = ExecutionLock(lock_path)
        assert new_lock.acquire() is True
        new_lock.release()
    
    @given(
        lock_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
            min_size=1,
            max_size=20
        )
    )
    @settings(max_examples=20)
    def test_lock_info_contains_pid_and_timestamp(self, lock_name: str):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        For any acquired lock, the lock info should contain PID and timestamp
        for debugging purposes.
        """
        lock_path = self.get_lock_path(lock_name)
        
        lock = ExecutionLock(lock_path)
        assert lock.acquire() is True
        
        # Get lock info from another instance
        other_lock = ExecutionLock(lock_path)
        lock_info = other_lock.get_lock_info()
        
        assert lock_info is not None
        assert 'pid' in lock_info
        assert 'timestamp' in lock_info
        assert lock_info['pid'] == str(os.getpid())
        
        lock.release()
    
    @given(
        num_sequential=st.integers(min_value=2, max_value=10)
    )
    @settings(max_examples=20)
    def test_sequential_lock_acquisitions(self, num_sequential: int):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        For any number of sequential lock acquire/release cycles, each should
        succeed independently.
        """
        lock_path = self.get_lock_path(f"sequential_{num_sequential}")
        
        for i in range(num_sequential):
            lock = ExecutionLock(lock_path)
            assert lock.acquire() is True, f"Acquisition {i} failed"
            assert lock.is_locked() is True
            lock.release()
            assert lock.is_locked() is False
    
    def test_context_manager_raises_on_lock_failure(self):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        When using context manager and lock is already held, a clear error
        should be raised.
        """
        lock_path = self.get_lock_path("context_error")
        
        # Acquire first lock
        first_lock = ExecutionLock(lock_path)
        assert first_lock.acquire() is True
        
        try:
            # Second lock via context manager should raise
            with pytest.raises(LockAcquisitionError) as exc_info:
                with ExecutionLock(lock_path):
                    pass
            
            # Error message should indicate ETL is already running
            assert "already running" in str(exc_info.value).lower()
        finally:
            first_lock.release()
    
    @given(
        lock_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_"),
            min_size=1,
            max_size=20
        )
    )
    @settings(max_examples=20)
    def test_lock_file_created_in_correct_location(self, lock_name: str):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        For any lock path, the lock file should be created at the specified location.
        """
        lock_path = self.get_lock_path(lock_name)
        
        # Lock file shouldn't exist yet
        assert not os.path.exists(lock_path)
        
        lock = ExecutionLock(lock_path)
        assert lock.acquire() is True
        
        # Lock file should now exist
        assert os.path.exists(lock_path)
        
        lock.release()
    
    def test_lock_handles_nested_directories(self):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        Lock should create parent directories if they don't exist.
        """
        nested_path = os.path.join(self.temp_dir, "nested", "dirs", "test.lock")
        
        lock = ExecutionLock(nested_path)
        assert lock.acquire() is True
        assert os.path.exists(nested_path)
        
        lock.release()
    
    @given(
        num_threads=st.integers(min_value=3, max_value=8),
        hold_time_ms=st.integers(min_value=10, max_value=100)
    )
    @settings(max_examples=10, deadline=None)
    def test_lock_fairness_under_contention(self, num_threads: int, hold_time_ms: int):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        Under contention, locks should be acquired fairly (no starvation).
        """
        lock_path = self.get_lock_path(f"fairness_{num_threads}")
        acquisition_order: List[int] = []
        order_lock = threading.Lock()
        barrier = threading.Barrier(num_threads)
        
        def contend_for_lock(thread_id: int):
            """Contend for lock and record acquisition order."""
            # Wait for all threads to be ready
            barrier.wait()
            
            lock = ExecutionLock(lock_path)
            
            # Keep trying until we get the lock
            max_attempts = 100
            for _ in range(max_attempts):
                if lock.acquire():
                    with order_lock:
                        acquisition_order.append(thread_id)
                    
                    # Hold lock briefly
                    time.sleep(hold_time_ms / 1000.0)
                    lock.release()
                    return
                
                # Small delay before retry
                time.sleep(0.01)
        
        # Run threads
        threads = [
            threading.Thread(target=contend_for_lock, args=(i,))
            for i in range(num_threads)
        ]
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join(timeout=10)
        
        # All threads should have acquired the lock eventually
        assert len(acquisition_order) == num_threads, \
            f"Expected {num_threads} acquisitions, got {len(acquisition_order)}"
        
        # All thread IDs should be present (no starvation)
        assert set(acquisition_order) == set(range(num_threads)), \
            "Some threads were starved"
    
    def test_double_release_is_safe(self):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        Releasing a lock multiple times should be safe (no errors).
        """
        lock_path = self.get_lock_path("double_release")
        
        lock = ExecutionLock(lock_path)
        assert lock.acquire() is True
        
        # Multiple releases should not raise
        lock.release()
        lock.release()  # Should be safe
        lock.release()  # Should be safe
        
        assert lock.is_locked() is False
    
    def test_release_without_acquire_is_safe(self):
        """
        Feature: trestle-etl-pipeline, Property 16: Execution Lock Prevention
        
        Releasing a lock that was never acquired should be safe.
        """
        lock_path = self.get_lock_path("no_acquire_release")
        
        lock = ExecutionLock(lock_path)
        
        # Release without acquire should not raise
        lock.release()
        assert lock.is_locked() is False
