class Damon < Formula
  desc "Local multi-provider agent daemon"
  homepage "https://github.com/developjik/damon-agent-core"
  version "0.2.0"
  license "MIT OR Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.2.0/damon-aarch64-apple-darwin.tar.gz"
      sha256 "f0d4cf8245440ada666ae4ac5b004a071e1a558e2dcfe898d4b8427d107dcbea"
    end
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.2.0/damon-x86_64-apple-darwin.tar.gz"
      sha256 "bbac167bb38dfeec89ebb83be9a07ff275e7212c313684bad83a28ce219726b4"
    end
  end
  on_linux do
    on_intel do
      url "https://github.com/developjik/damon-agent-core/releases/download/v0.2.0/damon-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "06659c2ccee688aa5bcdb7ce66103f8fc7a26e907076627af9d056bab9f75d68"
    end
    on_arm do
      # No aarch64 Linux tarball is published — fail loudly rather than
      # install an x86_64 binary that cannot run.
      odie "damon has no aarch64 Linux build; install via npm or cargo"
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
