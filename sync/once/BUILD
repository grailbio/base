load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["once.go"],
    importpath = "github.com/grailbio/base/sync/once",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["once_test.go"],
    embed = [":go_default_library"],
)
