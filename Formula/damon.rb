class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.6.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.6.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "1f87c8bf07cdd519be79def9db4e6ecf8c9c710121f788d0386499dcb04edb20"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.6.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "33b990e1a54064ce762889ee7397c898b9d3a01666726a5274270e884145c52b"
    end
  end
  on_linux do
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.6.0/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "c8ac822e4234c5c8891d5cb5480f50db13cb89a1e51b89ec76d995bab9275b44"
    end
    on_arm do
      # The sha256 is a placeholder — the release workflow rewrites it
      # from the published tarball on every tag ("Update Formula
      # sha256" step in .github/workflows/ci.yml).
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.6.0/damon-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "ee075a28aa3ff2ca2ffd6bb589a2447db8fe5859bdd0cbc0ffe4283a91fd655a"
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
