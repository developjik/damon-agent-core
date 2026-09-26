class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.5.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.5.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "54b18ebc0bae8a863008c4ddce607d786704c6d7383f42b165c28c2085cd78de"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.5.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "f0b744ecd16a142f84d5fd0425b81f4b50e7296d89c996e687cfaa187bbabcbe"
    end
  end
  on_linux do
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.5.0/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "fa0c7fb76cd3288f4625aa9f2b925d1077911ecd1f25aa2e94e73fddabb5d179"
    end
    on_arm do
      # The sha256 is a placeholder — the release workflow rewrites it
      # from the published tarball on every tag ("Update Formula
      # sha256" step in .github/workflows/ci.yml).
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.5.0/damon-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "1cdfd2e509d63574c9203663ca767e2a699a5b9e11efc95a34a4506b805847e7"
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
