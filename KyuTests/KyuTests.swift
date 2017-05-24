//
//  KyuTests.swift
//  KyuTests
//
//  Created by Red Davis on 14/06/2015.
//  Copyright (c) 2015 Red Davis. All rights reserved.
//

import XCTest
import Kyu


// MARK: - Test Worker

class TestJob: KyuJobProtocol
{
    func perform(_ arguments: [String : AnyObject]) -> KyuJobResult
    {
        return KyuJobResult.success
    }
}

class FailingTestJob: KyuJobProtocol
{
    func perform(_ arguments: [String : AnyObject]) -> KyuJobResult
    {
        return KyuJobResult.fail
    }
}

// MARK: - New Line Worker

class NewLineJob: KyuJobProtocol
{
    static let filePathArgumentKey = "filePath"
    
    func perform(_ arguments: [String : AnyObject]) -> KyuJobResult
    {
        guard let filePath = arguments[NewLineJob.filePathArgumentKey] as? String,
              let fileHandle = FileHandle(forWritingAtPath: filePath) else
        {
            return KyuJobResult.fail
        }
        
        let stringToWrite = "Hello\n"
        let stringToWriteData = stringToWrite.data(using: String.Encoding.utf8)!
        
        fileHandle.seekToEndOfFile()
        fileHandle.write(stringToWriteData)
        fileHandle.closeFile()
        
        return KyuJobResult.success
    }
}


class KyuTests: XCTestCase, KyuDataSource
{
    fileprivate var kyu: Kyu!
    fileprivate let operationQueue = OperationQueue()
    fileprivate var shouldIncrementRetryCount = true
    
    // MARK: Setup
    
    override func setUp()
    {
        super.setUp()
        
        self.shouldIncrementRetryCount = true
        
        let kyuQueueURL = self.randomQueueURL()
        self.kyu = try! Kyu(numberOfWorkers: 4, job: TestJob(), directoryURL: kyuQueueURL, maximumNumberOfRetries: 0)
    }
    
    // MARK: Tests
    
    func testInitializationHelper()
    {
        do
        {
            let _ = try Kyu.configure { (config: KyuConfiguration) -> () in
                config.numberOfWorkers = 1
                config.directoryURL = self.randomQueueURL()
                config.job = TestJob()
            }
        }
        catch let error
        {
            XCTFail("Invalid error raised \(error)")
        }
    }
    
    func testErrorRaisedWithInvalidNumberOfThreads()
    {
        do
        {
            let _ = try Kyu(numberOfWorkers: 0, job: TestJob(), directoryURL: self.randomQueueURL(), maximumNumberOfRetries: 0)
            XCTFail("KyuError.InvalidNumberOfThreads should have been raised")
        }
        catch KyuError.invalidNumberOfWorkers
        {
            XCTAssert(true)
        }
        catch
        {
            XCTFail("Wrong error raised")
        }
    }
    
    func testKyuCountingJobs()
    {
        self.kyu.paused = true
        self.kyu.queueJob(["1":2, "3":4])
        self.kyu.queueJob(["1":2, "3":4])
        self.kyu.queueJob(["1":2, "3":4])
        
        let expectation = self.expectation(description: "check number of jobs")
        self.operationQueue.addOperation { () -> Void in
            while true
            {
                if self.kyu.numberOfJobs == 3
                {
                    expectation.fulfill()
                    break
                }
            }
        }
        
        self.waitForExpectations(timeout: 3.0, handler: nil)
    }
    
    func testOutputShouldContain4NewLines()
    {
        // Create temp file
        let resultFileDirectoryPath = self.randomQueueURL().path
        let resultFilePath = resultFileDirectoryPath + "/result.txt"
        
        try! FileManager.default.createDirectory(atPath: resultFileDirectoryPath, withIntermediateDirectories: true, attributes: nil)
        FileManager.default.createFile(atPath: resultFilePath, contents: nil, attributes: nil)
        
        // Setup Kyu
        let queueURL = self.randomQueueURL()
        let newLineKyu = try! Kyu(numberOfWorkers: 4, job: NewLineJob(), directoryURL: queueURL, maximumNumberOfRetries: 0)
        
        newLineKyu.queueJob([NewLineJob.filePathArgumentKey:resultFilePath])
        newLineKyu.queueJob([NewLineJob.filePathArgumentKey:resultFilePath])
        newLineKyu.queueJob([NewLineJob.filePathArgumentKey:resultFilePath])
        
        let expectation = self.expectation(description: "write all lines")
        
        self.operationQueue.addOperation { () -> Void in
            sleep(1) // Give time to write data to disc :/
            
            while true
            {
                if newLineKyu.numberOfJobs == 0
                {
                    let resultFileData = try! Data(contentsOf: URL(fileURLWithPath: resultFilePath))
                    let resultString = NSString(data: resultFileData, encoding: String.Encoding.utf8.rawValue)!
                    
                    let resultLines = resultString.components(separatedBy: "\n")
                    XCTAssertEqual(resultLines.count, 4)
                    
                    expectation.fulfill()
                    break
                }
            }
        }
        
        self.waitForExpectations(timeout: 10.0, handler: nil)
    }
    
    func testNotIncrementingRetryCount()
    {
        self.shouldIncrementRetryCount = false
        
        // Setup Kyu
        let queueURL = self.randomQueueURL()
        let kyu = try! Kyu(numberOfWorkers: 4, job: FailingTestJob(), directoryURL: queueURL, maximumNumberOfRetries: 0)
        kyu.dataSource = self
        
        // Add job
        kyu.queueJob(["1":2])
        
        let expectation = self.expectation(description: "shouldn't delete job")
        
        self.operationQueue.addOperation { () -> Void in
            sleep(1) // Give time to write data to disc :/
            
            XCTAssertEqual(kyu.numberOfJobs, 1)
            expectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 10.0, handler: nil)
    }
    
    // MARK: Cancelling jobs
    
    func testCancellingJob()
    {
        self.kyu.paused = true
        
        // Add job
        let jobIdentifier = self.kyu.queueJob(["1":2])
        
        let expectation = self.expectation(description: "cancel job")
        
        self.operationQueue.addOperation { () -> Void in
            sleep(1) // Give time to write data to disc :/
            
            XCTAssertEqual(self.kyu.numberOfJobs, 1)
            
            try! self.kyu.cancelJob(jobIdentifier)
            
            XCTAssertEqual(self.kyu.numberOfJobs, 0)
            expectation.fulfill()
        }
        
        self.waitForExpectations(timeout: 10.0, handler: nil)
    }
    
    func testCancellingJobThatDoesntExist()
    {
        do
        {
            try self.kyu.cancelJob("i made this up")
            XCTFail("KyuJobManagementError.JobNotFound should have been raised")
        }
        catch KyuJobManagementError.jobNotFound
        {
            XCTAssert(true)
        }
        catch
        {
            XCTFail("Wrong error raised")
        }
    }
    
    // MARK: Helpers
    
    fileprivate func randomQueueURL() -> URL
    {
        return URL(string: NSTemporaryDirectory() + "\(arc4random())\(arc4random())")!
    }
    
    // MARK: KyuDataSource
    
    func kyuShouldIncrementRetryCount() -> Bool
    {
        return self.shouldIncrementRetryCount
    }
}
