class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.4.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.4.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "06f0880225d6d0c80ca80306368ada092b1cf5ca38512ce4171f4bba6e4bc887"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.4.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "7304ceae2a7840c4e8537a62f8973129c82066aacacfbfd6968e52461aa75dde"
    end
  end
  on_linux do
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.4.0/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "7773ac7e946e7f42fa1c4a534162848aa76832f811ea508158b4615c327a1409"
    end
    on_arm do
      # The sha256 is a placeholder — the release workflow rewrites it
      # from the published tarball on every tag ("Update Formula
      # sha256" step in .github/workflows/ci.yml).
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.4.0/damon-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "0000000000000000000000000000000000000000000000000000000000000000"
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
