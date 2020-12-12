require 'http'
require 'nokogiri'
require 'parallel'
require 'json'
require 'byebug'


def get_max_page(uri)
  content = HTTP.get(uri).to_s
  Nokogiri::HTML(content).css('.pageCount > span:nth-child(2)').text.to_i
end


PROVINCES = [
  'Manitoba+MB',
  'Alberta+AB',
  'Saskatchewan+SK',
  'British+Columbia+BC',
  'Ontario+ON'
]

uris = Enumerator.new do |yielder|
  PROVINCES.each do |province|
    max_page = get_max_page("https://www.yellowpages.ca/search/si/1/pharmacy/#{province}")
    (1..max_page).each do |page|
      yielder << URI("https://www.yellowpages.ca/search/si/#{page}/pharmacy/#{province}")
    end

    max_page = get_max_page("https://www.yellowpages.ca/search/si/1/police/#{province}")
    (1..max_page).each do |page|
      yielder << URI("https://www.yellowpages.ca/search/si/#{page}/police/#{province}")
    end

    max_page = get_max_page("https://www.yellowpages.ca/search/si/1/tim+hortons/#{province}")
    (1..max_page).each do |page|
      yielder << URI("https://www.yellowpages.ca/search/si/#{page}/tim+hortons/#{province}")
    end
  end
end

results = Parallel.map(uris, progress: 'Crawling Yellow Pages', ) do |uri|
  content = HTTP.get(uri).to_s

  next if content.include?('We didn’t find any business listings matching')

  addresses =
    Nokogiri::HTML(content)
      .css('[itemprop="address"]')
      .map do |e|
        {
          StreetAddress: e.css('[itemprop="streetAddress"]').text,
          AddressRegion: e.css('[itemprop="addressRegion"]').text,
          AddressLocality: e.css('[itemprop="addressLocality"]').text,
          PostalCode: e.css('[itemprop="postalCode"]').text
        }
      end
end

File.open('bc+ab+sk+mb+on.json', 'a') do |io|
  io.puts({ results: results.flatten.compact }.to_json)
end
