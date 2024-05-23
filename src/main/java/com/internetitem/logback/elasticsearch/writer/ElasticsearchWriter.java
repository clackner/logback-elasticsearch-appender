package com.internetitem.logback.elasticsearch.writer;

import com.internetitem.logback.elasticsearch.config.HttpRequestHeader;
import com.internetitem.logback.elasticsearch.config.HttpRequestHeaders;
import com.internetitem.logback.elasticsearch.config.Settings;
import com.internetitem.logback.elasticsearch.util.ErrorReporter;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;

public class ElasticsearchWriter implements SafeWriter {

	private StringBuilder sendBuffer;

	private ErrorReporter errorReporter;
	private Settings settings;
	private Collection<HttpRequestHeader> headerList;

	private boolean bufferExceeded;

	public ElasticsearchWriter(ErrorReporter errorReporter, Settings settings, HttpRequestHeaders headers) {
		this.errorReporter = errorReporter;
		this.settings = settings;
		this.headerList = headers != null && headers.getHeaders() != null
				? headers.getHeaders()
				: Collections.<HttpRequestHeader>emptyList();

		this.sendBuffer = new StringBuilder();
	}

	public void write(char[] cbuf, int off, int len) {
		if (bufferExceeded) {
			return;
		}

		sendBuffer.append(cbuf, off, len);

		if (sendBuffer.length() >= settings.getMaxQueueSize()) {
			errorReporter.logWarning("Send queue maximum size exceeded - log messages will be lost until the buffer is cleared");
			bufferExceeded = true;
		}
	}

	public void sendData() throws IOException {
		if (sendBuffer.length() <= 0) {
			return;
		}

		HttpURLConnection urlConnection = (HttpURLConnection) (settings.getUrl().openConnection());
		try {
			urlConnection.setDoInput(true);
			urlConnection.setDoOutput(true);
			urlConnection.setReadTimeout(settings.getReadTimeout());
			urlConnection.setConnectTimeout(settings.getConnectTimeout());
			urlConnection.setRequestMethod("POST");

			String body = sendBuffer.toString();

			if (!headerList.isEmpty()) {
				for (HttpRequestHeader header : headerList) {
					urlConnection.setRequestProperty(header.getName(), header.getValue());
				}
			}

			if (settings.getAuthentication() != null) {
				settings.getAuthentication().addAuth(urlConnection, body);
			}

			try (Writer writer = new OutputStreamWriter(urlConnection.getOutputStream(), StandardCharsets.UTF_8)) {
				writer.write(body);
				writer.flush();
			}

			int rc = urlConnection.getResponseCode();
			// response code is not within range 200-299
			if (!isSuccessful(rc)) {
				if (bufferExceeded || !canBeRetried(rc)) {
					// response code indicates that retrying won't resolve the issue.
					// in order to prevent any endless loop of retries happening, we reset the buffer immediately.
					resetBuffer();
				}

				final String data = slurpErrors(urlConnection);
				throw new IOException("Got response code [" + rc + "] from server with data " + data);
			}
		} finally {
			urlConnection.disconnect();
		}

		resetBuffer();
	}

	private void resetBuffer() {
		sendBuffer.setLength(0);
		if (bufferExceeded) {
			errorReporter.logInfo("Send queue cleared - log messages will no longer be lost");
			bufferExceeded = false;
		}
	}

	public boolean hasPendingData() {
		return !sendBuffer.isEmpty();
	}

	@Override
	public void reset() {
		resetBuffer();
	}

	private static String slurpErrors(HttpURLConnection urlConnection) {
		try (InputStream stream = urlConnection.getErrorStream()) {
			if (stream == null) {
				return "<no data>";
			}

			StringBuilder builder = new StringBuilder(2048);
			try (InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
				char[] buf = new char[2048];
				int numRead;
				while ((numRead = reader.read(buf)) > 0) {
					builder.append(buf, 0, numRead);
				}
			}
			return builder.toString();
		} catch (Exception e) {
			return "<error retrieving data: " + e.getMessage() + ">";
		}
	}

	private static boolean isSuccessful(final int httpStatus) {
		return httpStatus >= 200 && httpStatus < 300;
	}

	private static boolean canBeRetried(final int httpStatus) {
		return httpStatus == 408 || httpStatus == 429 || httpStatus < 400 || httpStatus >= 500;
	}
}
