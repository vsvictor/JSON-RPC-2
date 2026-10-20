import contextlib
import errno
import importlib.util
import io
import socket
import unittest
from pathlib import Path
from unittest.mock import Mock


spec = importlib.util.spec_from_file_location(
    "probe_transports", Path(__file__).with_name("probe-transports.py")
)
probe_transports = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe_transports)


class KernelTransportTests(unittest.TestCase):
    def test_supported_sockets_are_closed_for_both_families(self):
        factory = Mock(return_value=contextlib.nullcontext())
        self.assertEqual(
            probe_transports.probe("0.0.0.0,[::]", factory),
            ["udplite", "dccp", "sctp"],
        )
        self.assertEqual(factory.call_count, 6)
        self.assertEqual(
            {call.args[0] for call in factory.call_args_list},
            {socket.AF_INET, socket.AF_INET6},
        )

    def test_unsupported_transport_is_skipped_with_diagnostic(self):
        for code in probe_transports.UNSUPPORTED:
            def factory(family, kind, protocol):
                if protocol == 33:
                    raise OSError(code, "not supported")
                return contextlib.nullcontext()

            output = io.StringIO()
            with self.subTest(code=code), contextlib.redirect_stderr(output):
                self.assertEqual(
                    probe_transports.probe("0.0.0.0", factory), ["udplite", "sctp"]
                )
            self.assertIn("Skipping dccp and ws-dccp", output.getvalue())

    def test_permission_and_resource_errors_are_not_skipped(self):
        for code in (errno.EPERM, errno.EACCES, errno.EMFILE, errno.ENOBUFS):
            with self.subTest(code=code), self.assertRaises(OSError) as error:
                probe_transports.probe(
                    "0.0.0.0", Mock(side_effect=OSError(code, "probe failed"))
                )
            self.assertEqual(error.exception.errno, code)

    def test_ipv4_bind_does_not_require_ipv6_support(self):
        factory = Mock(return_value=contextlib.nullcontext())
        probe_transports.probe("0.0.0.0", factory)
        self.assertTrue(all(call.args[0] == socket.AF_INET for call in factory.call_args_list))

    def test_empty_bind_probes_default_families(self):
        factory = Mock(return_value=contextlib.nullcontext())
        probe_transports.probe("", factory)
        self.assertEqual(factory.call_count, 6)

    def test_invalid_bind_is_rejected_before_probing(self):
        factory = Mock()
        with self.assertRaises(ValueError):
            probe_transports.probe("invalid", factory)
        factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
