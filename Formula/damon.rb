class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.1.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "4b7086696288a0cc4e1e1f0de421aa8f2e4aa18c2105826fb30ce3a7412d34a2"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "21fd85310eda3a8863d5e578cd37a33dbdcfa6c39b8066291671426aa11eed68"
    end
  end
  on_linux do
    url "https://github.com/developjik/damon-agent-core/releases/download/v0.1.0/damon-x86_64-unknown-linux-gnu.tar.gz"
    sha256 "a814341151b85bcadd28ce9e09025824b0de563aebae9a504da8a50f02d534ac"
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
