class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.8.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.8.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "ed3573350bb8924789ecae7b785015f697fd987dc8ac2728fc85a1bee287d2c1"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.8.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "3175ab722b6b4132c3470065010938e90d66ce39d201b8f5f6f499e9f4eb578d"
    end
  end
  on_linux do
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.8.0/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "a82fb157f9262630d402ea07bc227c2ec18337387a26ea47f9faf2c8d8b21ef1"
    end
    on_arm do
      # The sha256 is a placeholder — the release workflow rewrites it
      # from the published tarball on every tag ("Update Formula
      # sha256" step in .github/workflows/ci.yml).
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.8.0/damon-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "85dc7652bc22fb4e242e08f1d9d1cb9a711eba5ed9c71eb6abb9a9d8cb997dfe"
    end
  end

  def install
    bin.install "damond"
    bin.install "damon"
    bin.install "damon-telegram"
    bin.install "damon-discord"
    bin.install "damon-slack"
    bin.install "damon-relay"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/damond --version")
    assert_match version.to_s, shell_output("#{bin}/damon --version")
  end
end
