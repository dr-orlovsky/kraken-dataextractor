RSpec.describe Main do

  context 'should work' do

    before :example do
      stub_request(:get, 'https://api.github.com/orgs/octokit/repos').
        to_return(status: 200, body: load_fixture_text('repos.json'))
    end

    subject do
      Main.new('octokit.py').run
    end

    it { is_expected.not_to be_nil }

  end
end
