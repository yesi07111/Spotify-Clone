import subprocess
import time
import sys
import Pyro5.api as rpc
from typing import List, Dict, Optional, Tuple

TIMEOUT = 40


class Colors:
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    BOLD = "\033[1m"


class RaftTestSuite:
    def __init__(self, num_nodes: int = 3):
        self.num_nodes = num_nodes
        self.processes: List[subprocess.Popen] = []
        self.nodes: List[rpc.Proxy] = []
        self.config = {i: ("localhost", 8000 + i) for i in range(1, num_nodes + 1)}
        self.test_results = []

    def log(self, message: str, color: str = Colors.RESET):
        print(f"{color}{message}{Colors.RESET}")

    def start_cluster(self):
        """Inicia un cluster de nodos Raft"""
        self.log(f"\n{'=' * 60}", Colors.BLUE)
        self.log(f"Iniciando cluster con {self.num_nodes} nodos...", Colors.BLUE)
        self.log(f"{'=' * 60}\n", Colors.BLUE)

        for node_id in range(1, self.num_nodes + 1):
            process = subprocess.Popen(
                [sys.executable, "raft.py", str(node_id)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            self.processes.append(process)
            host, port = self.config[node_id]
            self.nodes.append(rpc.Proxy(f"PYRO:raft.node.{node_id}@{host}:{port}"))

        time.sleep(3)
        self.log("✓ Cluster iniciado\n", Colors.GREEN)

    def stop_cluster(self):
        """Detiene todos los nodos del cluster"""
        self.log("\nDeteniendo cluster...", Colors.YELLOW)
        for process in self.processes:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
        self.processes.clear()
        self.nodes.clear()
        time.sleep(1)
        self.log("✓ Cluster detenido\n", Colors.GREEN)

    def stop_node(self, node_id: int):
        """Detiene un nodo específico"""
        if 0 < node_id <= len(self.processes):
            self.log(f"Deteniendo nodo {node_id}...", Colors.YELLOW)
            self.processes[node_id - 1].terminate()
            try:
                self.processes[node_id - 1].wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.processes[node_id - 1].kill()

    def restart_node(self, node_id: int):
        """Reinicia un nodo específico"""
        self.log(f"Reiniciando nodo {node_id}...", Colors.YELLOW)
        process = subprocess.Popen(
            [sys.executable, "raft.py", str(node_id)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.processes[node_id - 1] = process
        host, port = self.config[node_id]
        self.nodes[node_id - 1] = rpc.Proxy(f"PYRO:raft.node.{node_id}@{host}:{port}")
        time.sleep(2)

    def get_node_state(self, node_id: int) -> Optional[Dict]:
        """Obtiene el estado de un nodo"""
        try:
            node = self.nodes[node_id - 1]
            state = node.get_state()
            print(state)
            return state
        except Exception as e:
            return None

    def find_leader(self) -> Optional[int]:
        """Encuentra el líder actual del cluster"""
        time.sleep(1)
        for node_id in range(1, self.num_nodes + 1):
            state = self.get_node_state(node_id)
            if state and state["state"] == "leader":
                return node_id
        return None

    def wait_for_leader(self, timeout: int = 10) -> Optional[int]:
        """Espera hasta que se elija un líder"""
        start = time.time()
        while time.time() - start < timeout:
            leader = self.find_leader()
            if leader:
                return leader
            time.sleep(0.5)
        return None

    def verify_single_leader(self) -> Tuple[bool, str]:
        """Verifica que hay exactamente un líder"""
        leaders = []
        for node_id in range(1, self.num_nodes + 1):
            state = self.get_node_state(node_id)
            if state and state["state"] == "leader":
                leaders.append(node_id)

        if len(leaders) == 0:
            return False, "No hay líder en el cluster"
        elif len(leaders) > 1:
            return False, f"Múltiples líderes detectados: {leaders}"
        else:
            return True, f"Un único líder: Nodo {leaders[0]}"

    def verify_term_consistency(self) -> Tuple[bool, str]:
        """Verifica que todos los nodos están en el mismo término o términos cercanos"""
        terms = []
        for node_id in range(1, self.num_nodes + 1):
            state = self.get_node_state(node_id)
            if state:
                terms.append((node_id, state["current_term"]))

        if not terms:
            return False, "No se pudo obtener términos de ningún nodo"

        max_term = max(t[1] for t in terms)
        min_term = min(t[1] for t in terms)

        if max_term - min_term > 1:
            return False, f"Términos muy divergentes: {terms}"
        else:
            return True, f"Términos consistentes: {terms}"

    def send_client_request(self, leader_id: int, command: str) -> bool:
        """Envía una solicitud de cliente al líder"""
        try:
            response = self.nodes[leader_id - 1].client_request(command)
            return response.get("success", False)
        except Exception as e:
            return False

    # ============================================================
    # TESTS
    # ============================================================

    def test_initial_leader_election(self):
        """Test 1: Elección inicial de líder"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 1: Elección inicial de líder", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()

        leader = self.wait_for_leader(timeout=TIMEOUT)
        if leader:
            self.log(f"✓ Líder elegido: Nodo {leader}", Colors.GREEN)
            success, msg = self.verify_single_leader()
            if success:
                self.log(f"✓ {msg}", Colors.GREEN)
                self.test_results.append(("Elección inicial", True))
            else:
                self.log(f"✗ {msg}", Colors.RED)
                self.test_results.append(("Elección inicial", False))
        else:
            self.log("✗ No se eligió líder en el tiempo esperado", Colors.RED)
            self.test_results.append(("Elección inicial", False))

        self.stop_cluster()

    def test_leader_failure_reelection(self):
        """Test 2: Re-elección tras fallo del líder"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 2: Re-elección tras fallo del líder", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()

        first_leader = self.wait_for_leader(timeout=TIMEOUT)
        if not first_leader:
            self.log("✗ No se eligió líder inicial", Colors.RED)
            self.test_results.append(("Re-elección", False))
            self.stop_cluster()
            return

        self.log(f"Líder inicial: Nodo {first_leader}", Colors.BLUE)
        self.log(f"Deteniendo líder (Nodo {first_leader})...", Colors.YELLOW)
        self.stop_node(first_leader)

        time.sleep(2)
        new_leader = self.wait_for_leader(timeout=TIMEOUT)

        if new_leader and new_leader != first_leader:
            self.log(f"✓ Nuevo líder elegido: Nodo {new_leader}", Colors.GREEN)
            success, msg = self.verify_single_leader()
            if success:
                self.log(f"✓ {msg}", Colors.GREEN)
                self.test_results.append(("Re-elección", True))
            else:
                self.log(f"✗ {msg}", Colors.RED)
                self.test_results.append(("Re-elección", False))
        else:
            self.log("✗ No se eligió nuevo líder o es el mismo", Colors.RED)
            self.test_results.append(("Re-elección", False))

        self.stop_cluster()

    def test_follower_failure_stability(self):
        """Test 3: Estabilidad ante fallo de follower"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 3: Estabilidad ante fallo de follower", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()

        leader = self.wait_for_leader(timeout=TIMEOUT)
        if not leader:
            self.log("✗ No se eligió líder inicial", Colors.RED)
            self.test_results.append(("Estabilidad follower", False))
            self.stop_cluster()
            return

        follower = 1 if leader != 1 else 2
        self.log(
            f"Líder: Nodo {leader}, deteniendo follower: Nodo {follower}", Colors.BLUE
        )
        self.stop_node(follower)

        time.sleep(5)
        current_leader = self.find_leader()

        if current_leader == leader:
            self.log(f"✓ El líder se mantiene estable: Nodo {leader}", Colors.GREEN)
            self.test_results.append(("Estabilidad follower", True))
        else:
            self.log(
                f"✗ El líder cambió inesperadamente: {leader} -> {current_leader}",
                Colors.RED,
            )
            self.test_results.append(("Estabilidad follower", False))

        self.stop_cluster()

    def test_term_consistency(self):
        """Test 4: Consistencia de términos"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 4: Consistencia de términos", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()
        self.wait_for_leader(timeout=TIMEOUT)

        time.sleep(5)
        success, msg = self.verify_term_consistency()

        if success:
            self.log(f"✓ {msg}", Colors.GREEN)
            self.test_results.append(("Consistencia términos", True))
        else:
            self.log(f"✗ {msg}", Colors.RED)
            self.test_results.append(("Consistencia términos", False))

        self.stop_cluster()

    def test_multiple_failures(self):
        """Test 5: Múltiples fallos secuenciales"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 5: Múltiples fallos secuenciales", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()

        failures = []
        for i in range(2):
            leader = self.wait_for_leader(timeout=TIMEOUT)
            if not leader:
                self.log(f"✗ No hay líder en iteración {i + 1}", Colors.RED)
                failures.append(i + 1)
                break

            self.log(f"Iteración {i + 1}: Líder = Nodo {leader}", Colors.BLUE)
            self.stop_node(leader)
            time.sleep(2)

        final_leader = self.wait_for_leader(timeout=TIMEOUT)
        if final_leader and not failures:
            self.log(
                f"✓ Cluster se recuperó exitosamente, líder final: Nodo {final_leader}",
                Colors.GREEN,
            )
            self.test_results.append(("Múltiples fallos", True))
        else:
            self.log("✗ El cluster no se recuperó correctamente", Colors.RED)
            self.test_results.append(("Múltiples fallos", False))

        self.stop_cluster()

    def test_split_brain_prevention(self):
        """Test 6: Prevención de split-brain"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 6: Prevención de split-brain", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()
        self.wait_for_leader(timeout=TIMEOUT)

        time.sleep(2)
        samples = 5
        violations = 0

        for i in range(samples):
            success, msg = self.verify_single_leader()
            if not success:
                violations += 1
                self.log(f"Muestra {i + 1}: ✗ {msg}", Colors.RED)
            else:
                self.log(f"Muestra {i + 1}: ✓ {msg}", Colors.GREEN)
            time.sleep(1)

        if violations == 0:
            self.log("✓ No se detectó split-brain", Colors.GREEN)
            self.test_results.append(("Split-brain", True))
        else:
            self.log(f"✗ Detectadas {violations}/{samples} violaciones", Colors.RED)
            self.test_results.append(("Split-brain", False))

        self.stop_cluster()

    def test_node_recovery(self):
        """Test 7: Recuperación de nodo caído"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 7: Recuperación de nodo caído", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()
        leader = self.wait_for_leader(timeout=TIMEOUT)

        if not leader:
            self.log("✗ No se eligió líder inicial", Colors.RED)
            self.test_results.append(("Recuperación nodo", False))
            self.stop_cluster()
            return

        follower = 1 if leader != 1 else 2
        self.log(f"Deteniendo follower: Nodo {follower}", Colors.YELLOW)
        self.stop_node(follower)
        time.sleep(3)

        self.log(f"Reiniciando nodo {follower}...", Colors.YELLOW)
        self.restart_node(follower)
        time.sleep(5)

        recovered_state = self.get_node_state(follower)
        if recovered_state:
            self.log(f"✓ Nodo {follower} recuperado: {recovered_state}", Colors.GREEN)
            if recovered_state["state"] in ["follower", "candidate"]:
                self.test_results.append(("Recuperación nodo", True))
            else:
                self.log(f"✗ Estado inesperado: {recovered_state['state']}", Colors.RED)
                self.test_results.append(("Recuperación nodo", False))
        else:
            self.log(f"✗ No se pudo recuperar nodo {follower}", Colors.RED)
            self.test_results.append(("Recuperación nodo", False))

        self.stop_cluster()

    def test_rapid_leader_changes(self):
        """Test 8: Cambios rápidos de líder"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("TEST 8: Cambios rápidos de líder", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        self.start_cluster()

        leaders_seen = set()
        for i in range(3):
            leader = self.wait_for_leader(timeout=10)
            if leader:
                leaders_seen.add(leader)
                self.log(f"Cambio {i + 1}: Líder = Nodo {leader}", Colors.BLUE)
                self.stop_node(leader)
                time.sleep(2)
            else:
                break

        if len(leaders_seen) >= 2:
            self.log(
                f"✓ Cluster manejó {len(leaders_seen)} cambios de líder", Colors.GREEN
            )
            self.test_results.append(("Cambios rápidos", True))
        else:
            self.log("✗ No se observaron suficientes cambios de líder", Colors.RED)
            self.test_results.append(("Cambios rápidos", False))

        self.stop_cluster()

    def run_all_tests(self):
        """Ejecuta todos los tests"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("INICIANDO SUITE DE TESTS PARA RAFT", Colors.BOLD)
        self.log("=" * 60 + "\n", Colors.BOLD)

        try:
            self.test_initial_leader_election()
            self.test_leader_failure_reelection()
            self.test_follower_failure_stability()
            self.test_term_consistency()
            self.test_multiple_failures()
            self.test_split_brain_prevention()
            self.test_node_recovery()
            self.test_rapid_leader_changes()
        finally:
            self.print_summary()

    def print_summary(self):
        """Imprime resumen de resultados"""
        self.log("\n" + "=" * 60, Colors.BOLD)
        self.log("RESUMEN DE TESTS", Colors.BOLD)
        self.log("=" * 60, Colors.BOLD)

        passed = sum(1 for _, result in self.test_results if result)
        total = len(self.test_results)

        for test_name, result in self.test_results:
            status = "✓ PASS" if result else "✗ FAIL"
            color = Colors.GREEN if result else Colors.RED
            self.log(f"{status}: {test_name}", color)

        self.log("\n" + "-" * 60, Colors.BOLD)
        percentage = (passed / total * 100) if total > 0 else 0
        color = (
            Colors.GREEN
            if percentage >= 80
            else Colors.YELLOW
            if percentage >= 60
            else Colors.RED
        )
        self.log(f"Total: {passed}/{total} tests pasados ({percentage:.1f}%)", color)
        self.log("=" * 60 + "\n", Colors.BOLD)


if __name__ == "__main__":
    test_suite = RaftTestSuite(num_nodes=10)
    try:
        test_suite.run_all_tests()
    except KeyboardInterrupt:
        print("\n\nTests interrumpidos por el usuario")
        test_suite.stop_cluster()
    except Exception as e:
        print(f"\n\nError durante los tests: {e}")
        test_suite.stop_cluster()
        raise e
