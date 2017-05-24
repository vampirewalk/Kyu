//
//  KyuJobTests.swift
//  Kyu
//
//  Created by Red Davis on 09/02/2016.
//  Copyright © 2016 Red Davis. All rights reserved.
//

import XCTest
@testable import Kyu


class KyuJobTests: XCTestCase
{
    // Private
    fileprivate var testJobURL: URL!
    
    // MARK: Setup
    
    override func setUp()
    {
        super.setUp()
        
        self.testJobURL = self.createTestJob()
    }
    
    override func tearDown()
    {
        super.tearDown()
    }
    
    // MARK: Retry count
    
    func testRetryCountIncreases()
    {
        let job = try! KyuJob(directoryURL: self.testJobURL)
        
        XCTAssertEqual(job.numberOfRetries, 0)
        
        job.incrementRetryCount()
        job.incrementRetryCount()
        job.incrementRetryCount()
        
        XCTAssertEqual(job.numberOfRetries, 3)
    }
    
    // MARK: Incrementing process date
    
    func testProcessDateIncreases()
    {
        let job = try! KyuJob(directoryURL: self.testJobURL)
        let originalProcessDate = job.processDate
        
        job.incrementRetryCount()
        job.incrementRetryCount()
        job.incrementRetryCount()
        
        let originalIsDateEarlier = originalProcessDate.compare(job.processDate) == .orderedAscending
        XCTAssert(originalIsDateEarlier)
    }
    
    // MARK: Helpers
    
    fileprivate func createTestJob() -> URL
    {
        let directoryName = UUID().uuidString
        let directoryPath = NSTemporaryDirectory() + directoryName
        
        let fileManager = FileManager.default
        
        try! fileManager.createDirectory(atPath: directoryPath, withIntermediateDirectories: true, attributes: nil)
        
        let directoryPathURL = URL(fileURLWithPath: directoryPath)
        let identifier = UUID().uuidString
        KyuJob.createJob(identifier, arguments: ["test" : "test"], queueDirectoryURL: directoryPathURL)
        
        let jobDirectoryName = try! fileManager.contentsOfDirectory(atPath: directoryPath).first!
        
        return directoryPathURL.appendingPathComponent(jobDirectoryName)
    }
}
