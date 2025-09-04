"""
Test DAG integrity for BarAlgae data infrastructure.

This module contains tests to ensure DAGs are properly configured
and can be parsed without errors.
"""

import pytest
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow.models import DagBag
from airflow.utils.dag_cycle import check_cycle


class TestDAGIntegrity:
    """Test class for DAG integrity checks."""
    
    def test_dag_imports(self):
        """Test that all DAGs can be imported without errors."""
        dag_bag = DagBag()
        assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
    
    def test_dag_structure(self):
        """Test that DAGs have proper structure."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has tasks
            assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"
            
            # Check that DAG has proper default args
            assert dag.default_args is not None, f"DAG {dag_id} has no default_args"
            
            # Check that DAG has proper schedule
            assert dag.schedule_interval is not None, f"DAG {dag_id} has no schedule_interval"
    
    def test_dag_no_cycles(self):
        """Test that DAGs have no cycles."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check for cycles
            cycle = check_cycle(dag)
            assert cycle is None, f"DAG {dag_id} has a cycle: {cycle}"
    
    def test_dag_task_dependencies(self):
        """Test that DAG tasks have proper dependencies."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that all tasks have proper dependencies
            for task in dag.tasks:
                assert task.dag == dag, f"Task {task.task_id} in DAG {dag_id} has wrong DAG reference"
    
    def test_dag_tags(self):
        """Test that DAGs have proper tags."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has tags
            assert dag.tags is not None, f"DAG {dag_id} has no tags"
            assert len(dag.tags) > 0, f"DAG {dag_id} has empty tags"
    
    def test_dag_owner(self):
        """Test that DAGs have proper owner."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has owner
            assert dag.default_args.get('owner') is not None, f"DAG {dag_id} has no owner"
    
    def test_dag_retry_config(self):
        """Test that DAGs have proper retry configuration."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has retry configuration
            assert 'retries' in dag.default_args, f"DAG {dag_id} has no retries configuration"
            assert 'retry_delay' in dag.default_args, f"DAG {dag_id} has no retry_delay configuration"
    
    def test_dag_catchup(self):
        """Test that DAGs have proper catchup configuration."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has catchup configuration
            assert dag.catchup is not None, f"DAG {dag_id} has no catchup configuration"
    
    def test_dag_max_active_runs(self):
        """Test that DAGs have proper max_active_runs configuration."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has max_active_runs configuration
            assert dag.max_active_runs is not None, f"DAG {dag_id} has no max_active_runs configuration"
    
    def test_dag_description(self):
        """Test that DAGs have proper description."""
        dag_bag = DagBag()
        
        for dag_id, dag in dag_bag.dags.items():
            # Check that DAG has description
            assert dag.description is not None, f"DAG {dag_id} has no description"
            assert len(dag.description) > 0, f"DAG {dag_id} has empty description"
