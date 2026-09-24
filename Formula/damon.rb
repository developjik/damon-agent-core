class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.3.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.3.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "9cf731aeb36eb1c750f7c79005b8c243366dabd6dec3e1bec7e38ddfe886d2ea"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.3.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "c4c9f1c67d79faffb1e51350c8f9c7fe8e924210f0a1e6ebd7590da47cd05616"
    end
  end
  on_linux do
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.3.0/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "d4a650ecb439bcc0bdc8b99433b499d16b577ab8704ad184c96e53cce8af5387"
    end
    on_arm do
      # The sha256 is a placeholder — the release workflow rewrites it
      # from the published tarball on every tag ("Update Formula
      # sha256" step in .github/workflows/ci.yml).
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.3.0/damon-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "2802bee909065dee9c86d6476a556ccdd2db5b3c34a51317d3af7ebabce5a194"
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
