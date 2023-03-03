load("@io_bazel_rules_go//go:def.bzl", _go_test = "go_test")

def go_test(**kwargs):
    _go_test(**kwargs)
    kwargs.pop("name")
    _go_test(
        name = "race_test",
        race = "on",
        # Run a smaller test under the race detector because execution is slower.
        args = ["-data-bytes={}".format(1 << 24), "-stress-parallelism=2"],
        **kwargs
    )
